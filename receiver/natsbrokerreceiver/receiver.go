package natsbrokerreceiver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/m3nowak/otelcol-nats/receiver/natsbrokerreceiver/internal/metadata"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/receiverhelper"
	"go.uber.org/zap"
)

const (
	varzPath = "/varz"
	jszPath  = "/jsz?consumers=true&config=true&raft=true"
)

type receiverComponent struct {
	cfg       *Config
	set       receiver.Settings
	next      consumer.Metrics
	obsreport *receiverhelper.ObsReport

	mu      sync.Mutex
	started bool
	client  *http.Client
	cancel  context.CancelFunc
	done    chan struct{}
}

func newReceiver(cfg *Config, set receiver.Settings, next consumer.Metrics) (*receiverComponent, error) {
	obsreport, err := receiverhelper.NewObsReport(receiverhelper.ObsReportSettings{
		ReceiverID:             component.NewID(metadata.Type),
		Transport:              "http",
		ReceiverCreateSettings: set,
	})
	if err != nil {
		return nil, err
	}

	return &receiverComponent{
		cfg:       cfg,
		set:       set,
		next:      next,
		obsreport: obsreport,
	}, nil
}

func (rcv *receiverComponent) Start(_ context.Context, _ component.Host) error {
	rcv.mu.Lock()
	defer rcv.mu.Unlock()
	if rcv.started {
		return nil
	}

	client, err := rcv.newHTTPClient()
	if err != nil {
		return err
	}

	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	rcv.client = client
	rcv.cancel = cancel
	rcv.done = done
	rcv.started = true

	go rcv.run(runCtx, done)

	rcv.set.Logger.Info("started NATS broker metrics receiver", zap.Strings("endpoints", rcv.cfg.Endpoints))
	return nil
}

func (rcv *receiverComponent) Shutdown(ctx context.Context) error {
	rcv.mu.Lock()
	if !rcv.started {
		rcv.mu.Unlock()
		return nil
	}
	cancel := rcv.cancel
	done := rcv.done
	rcv.cancel = nil
	rcv.done = nil
	rcv.client = nil
	rcv.started = false
	rcv.mu.Unlock()

	cancel()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (rcv *receiverComponent) run(ctx context.Context, done chan struct{}) {
	defer close(done)

	ticker := time.NewTicker(rcv.cfg.CollectionInterval)
	defer ticker.Stop()

	for {
		rcv.scrapeAndConsume(ctx)

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (rcv *receiverComponent) scrapeAndConsume(ctx context.Context) {
	metrics, err := rcv.scrape(ctx)
	if err != nil {
		rcv.set.Logger.Error("failed to scrape NATS broker metrics", zap.Error(err))
	}
	if metrics.MetricCount() == 0 {
		return
	}

	opCtx := rcv.obsreport.StartMetricsOp(ctx)
	err = rcv.next.ConsumeMetrics(opCtx, metrics)
	rcv.obsreport.EndMetricsOp(opCtx, "http", metrics.DataPointCount(), err)
	if err != nil {
		rcv.set.Logger.Error("failed to forward NATS broker metrics", zap.Error(err))
	}
}

func (rcv *receiverComponent) scrape(ctx context.Context) (pmetric.Metrics, error) {
	metrics := pmetric.NewMetrics()
	resourceMetrics := metrics.ResourceMetrics().AppendEmpty()
	scopeMetrics := resourceMetrics.ScopeMetrics().AppendEmpty()
	scopeMetrics.Scope().SetName("github.com/m3nowak/otelcol-nats/receiver/natsbrokerreceiver")
	builder := newMetricsBuilder(scopeMetrics.Metrics())
	timestamp := pcommon.NewTimestampFromTime(time.Now())

	var scrapeErr error
	for _, endpoint := range rcv.cfg.Endpoints {
		endpointID := strings.TrimSpace(endpoint)
		varzResp := map[string]any{}
		if err := rcv.getJSON(ctx, endpointID, varzPath, &varzResp); err != nil {
			scrapeErr = errors.Join(scrapeErr, fmt.Errorf("scrape %s%s: %w", endpointID, varzPath, err))
			continue
		}

		appendVarzMetrics(builder, endpointID, timestamp, varzResp)

		serverName, _ := varzResp["server_name"].(string)

		var jszResp jszResponse
		if err := rcv.getJSON(ctx, endpointID, jszPath, &jszResp); err != nil {
			rcv.set.Logger.Debug("failed to scrape JetStream metrics", zap.String("endpoint", endpointID), zap.Error(err))
			continue
		}
		appendJszMetrics(builder, endpointID, serverName, timestamp, jszResp)
	}

	return metrics, scrapeErr
}

func (rcv *receiverComponent) getJSON(parent context.Context, endpoint, path string, out any) error {
	ctx, cancel := context.WithTimeout(parent, rcv.cfg.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, joinEndpointPath(endpoint, path), nil)
	if err != nil {
		return err
	}
	if rcv.cfg.Auth.Username != "" {
		req.SetBasicAuth(rcv.cfg.Auth.Username, string(rcv.cfg.Auth.Password))
	}

	resp, err := rcv.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("unexpected status code %d", resp.StatusCode)
	}

	return json.NewDecoder(resp.Body).Decode(out)
}

func (rcv *receiverComponent) newHTTPClient() (*http.Client, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	tlsConfig, err := rcv.cfg.TLS.LoadTLSConfig(context.Background())
	if err != nil {
		return nil, fmt.Errorf("load tls config: %w", err)
	}
	if tlsConfig != nil {
		transport.TLSClientConfig = tlsConfig
	}
	return &http.Client{
		Transport: transport,
		Timeout:   rcv.cfg.Timeout,
	}, nil
}

type metricsBuilder struct {
	metrics pmetric.MetricSlice
	index   map[string]pmetric.Metric
}

func newMetricsBuilder(metrics pmetric.MetricSlice) *metricsBuilder {
	return &metricsBuilder{
		metrics: metrics,
		index:   map[string]pmetric.Metric{},
	}
}

func (b *metricsBuilder) addGaugePoint(name, description string, timestamp pcommon.Timestamp, value float64, attrs map[string]string) {
	metric, ok := b.index[name]
	if !ok {
		metric = b.metrics.AppendEmpty()
		metric.SetName(name)
		metric.SetDescription(description)
		metric.SetEmptyGauge()
		b.index[name] = metric
	}

	point := metric.Gauge().DataPoints().AppendEmpty()
	point.SetTimestamp(timestamp)
	point.SetDoubleValue(value)

	keys := make([]string, 0, len(attrs))
	for key := range attrs {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	for _, key := range keys {
		point.Attributes().PutStr(key, attrs[key])
	}
}

func appendVarzMetrics(builder *metricsBuilder, endpointID string, timestamp pcommon.Timestamp, response map[string]any, prefix ...string) {
	skipFQN := map[string]struct{}{
		"leaf":                    {},
		"trusted_operators_claim": {},
		"cluster_tls_timeout":     {},
		"cluster_cluster_port":    {},
		"cluster_auth_timeout":    {},
		"gateway_port":            {},
		"gateway_auth_timeout":    {},
		"gateway_tls_timeout":     {},
		"gateway_connect_retries": {},
	}

	labelKeys := map[string]struct{}{
		"server_id":        {},
		"server_name":      {},
		"version":          {},
		"domain":           {},
		"leader":           {},
		"name":             {},
		"start":            {},
		"config_load_time": {},
	}

	for key, value := range response {
		fqn, path := fqName(key, prefix...)
		if _, ok := skipFQN[fqn]; ok {
			continue
		}

		switch typed := value.(type) {
		case float64:
			builder.addGaugePoint("gnatsd_varz_"+fqn, fqn, timestamp, typed, map[string]string{"server_id": endpointID})
		case string:
			if _, ok := labelKeys[key]; !ok {
				continue
			}
			if parsed, err := time.Parse(time.RFC3339Nano, typed); err == nil {
				builder.addGaugePoint("gnatsd_varz_"+fqn, fqn, timestamp, float64(parsed.UnixMilli()), map[string]string{"server_id": endpointID})
				continue
			}
			builder.addGaugePoint("gnatsd_varz_"+fqn, fqn, timestamp, 1, map[string]string{
				"server_id": endpointID,
				"value":     typed,
			})
		case map[string]any:
			appendVarzMetrics(builder, endpointID, timestamp, typed, path...)
		}
	}
}

func appendJszMetrics(builder *metricsBuilder, endpointID, serverName string, timestamp pcommon.Timestamp, response jszResponse) {
	if serverName == "" {
		serverName = response.ID
	}

	clusterName := ""
	clusterLeader := ""
	isMetaLeader := "true"
	if response.Meta != nil {
		clusterName = response.Meta.Name
		clusterLeader = response.Meta.Leader
		isMetaLeader = boolString(clusterLeader == serverName)
	}

	serverLabels := map[string]string{
		"server_id":      endpointID,
		"server_name":    serverName,
		"cluster":        clusterName,
		"domain":         response.Config.Domain,
		"meta_leader":    clusterLeader,
		"is_meta_leader": isMetaLeader,
	}

	builder.addGaugePoint("jetstream_server_jetstream_disabled", "JetStream disabled or not", timestamp, boolToFloat(response.Disabled), serverLabels)
	builder.addGaugePoint("jetstream_server_total_streams", "Total number of streams in JetStream", timestamp, float64(response.Streams), serverLabels)
	builder.addGaugePoint("jetstream_server_total_consumers", "Total number of consumers in JetStream", timestamp, float64(response.Consumers), serverLabels)
	builder.addGaugePoint("jetstream_server_total_messages", "Total number of stored messages in JetStream", timestamp, float64(response.Messages), serverLabels)
	builder.addGaugePoint("jetstream_server_total_message_bytes", "Total number of bytes stored in JetStream", timestamp, float64(response.Bytes), serverLabels)
	builder.addGaugePoint("jetstream_server_max_memory", "JetStream Max Memory", timestamp, float64(response.Config.MaxMemory), serverLabels)
	builder.addGaugePoint("jetstream_server_max_storage", "JetStream Max Storage", timestamp, float64(response.Config.MaxStore), serverLabels)

	for _, account := range response.AccountDetails {
		accountLabels := mergeLabels(serverLabels, map[string]string{
			"account":      account.Name,
			"account_name": account.Name,
			"account_id":   account.ID,
		})

		builder.addGaugePoint("jetstream_account_max_memory", "JetStream Account Max Memory in bytes", timestamp, float64(account.ReservedMemory), accountLabels)
		builder.addGaugePoint("jetstream_account_max_storage", "JetStream Account Max Storage in bytes", timestamp, float64(account.ReservedStore), accountLabels)
		builder.addGaugePoint("jetstream_account_memory_used", "Total number of bytes used by JetStream memory", timestamp, float64(account.Memory), accountLabels)
		builder.addGaugePoint("jetstream_account_storage_used", "Total number of bytes used by JetStream storage", timestamp, float64(account.Store), accountLabels)

		for _, stream := range account.Streams {
			streamLeader := ""
			isStreamLeader := "true"
			if stream.Cluster != nil {
				streamLeader = stream.Cluster.Leader
				isStreamLeader = boolString(streamLeader == serverName)
			}

			streamLabels := mergeLabels(accountLabels, map[string]string{
				"stream_name":       stream.Name,
				"stream_leader":     streamLeader,
				"is_stream_leader":  isStreamLeader,
				"stream_raft_group": stream.RaftGroup,
			})

			builder.addGaugePoint("jetstream_stream_total_messages", "Total number of messages from a stream", timestamp, float64(stream.State.Messages), streamLabels)
			builder.addGaugePoint("jetstream_stream_total_bytes", "Total stored bytes from a stream", timestamp, float64(stream.State.Bytes), streamLabels)
			builder.addGaugePoint("jetstream_stream_first_seq", "First sequence from a stream", timestamp, float64(stream.State.FirstSeq), streamLabels)
			builder.addGaugePoint("jetstream_stream_last_seq", "Last sequence from a stream", timestamp, float64(stream.State.LastSeq), streamLabels)
			builder.addGaugePoint("jetstream_stream_consumer_count", "Total number of consumers from a stream", timestamp, float64(stream.State.ConsumerCount), streamLabels)
			builder.addGaugePoint("jetstream_stream_subject_count", "Total number of subjects in a stream", timestamp, float64(stream.State.SubjectCount), streamLabels)
			if stream.Config != nil {
				builder.addGaugePoint("jetstream_stream_limit_bytes", "The maximum configured storage limit (in bytes) for a JetStream stream. A value of -1 indicates no limit.", timestamp, float64(stream.Config.MaxBytes), streamLabels)
				builder.addGaugePoint("jetstream_stream_limit_messages", "The maximum number of messages allowed in a JetStream stream as per its configuration. A value of -1 indicates no limit.", timestamp, float64(stream.Config.MaxMsgs), streamLabels)
			}

			for _, source := range stream.Sources {
				sourceLabels := mergeLabels(streamLabels, map[string]string{
					"source_name":    source.Name,
					"source_api":     source.externalAPI(),
					"source_deliver": source.externalDeliver(),
				})
				builder.addGaugePoint("jetstream_stream_source_lag", "Number of messages a stream source is behind", timestamp, float64(source.Lag), sourceLabels)
				builder.addGaugePoint("jetstream_stream_source_active_duration_ns", "Stream source active duration in nanoseconds (-1 indicates inactive)", timestamp, float64(source.Active), sourceLabels)
			}

			if stream.Mirror != nil {
				mirrorLabels := mergeLabels(streamLabels, map[string]string{
					"mirror_name":    stream.Mirror.Name,
					"mirror_api":     stream.Mirror.externalAPI(),
					"mirror_deliver": stream.Mirror.externalDeliver(),
				})
				builder.addGaugePoint("jetstream_stream_mirror_lag", "Number of messages a stream mirror is behind", timestamp, float64(stream.Mirror.Lag), mirrorLabels)
				builder.addGaugePoint("jetstream_stream_mirror_active_duration_ns", "Stream mirror active duration in nanoseconds (-1 indicates inactive)", timestamp, float64(stream.Mirror.Active), mirrorLabels)
			}

			for _, consumerInfo := range stream.Consumers {
				consumerLeader := ""
				isConsumerLeader := "true"
				if consumerInfo.Cluster != nil {
					consumerLeader = consumerInfo.Cluster.Leader
					isConsumerLeader = boolString(consumerLeader == serverName)
				}

				consumerDescription := ""
				if consumerInfo.Config != nil {
					consumerDescription = consumerInfo.Config.Description
				}

				consumerLabels := mergeLabels(streamLabels, map[string]string{
					"consumer_name":      consumerInfo.Name,
					"consumer_leader":    consumerLeader,
					"is_consumer_leader": isConsumerLeader,
					"consumer_desc":      consumerDescription,
				})

				builder.addGaugePoint("jetstream_consumer_delivered_consumer_seq", "Latest sequence number of a stream consumer", timestamp, float64(consumerInfo.Delivered.ConsumerSeq), consumerLabels)
				builder.addGaugePoint("jetstream_consumer_delivered_stream_seq", "Latest sequence number of a stream", timestamp, float64(consumerInfo.Delivered.StreamSeq), consumerLabels)
				builder.addGaugePoint("jetstream_consumer_num_ack_pending", "Number of pending acks from a consumer", timestamp, float64(consumerInfo.NumAckPending), consumerLabels)
				builder.addGaugePoint("jetstream_consumer_num_redelivered", "Number of redelivered messages from a consumer", timestamp, float64(consumerInfo.NumRedelivered), consumerLabels)
				builder.addGaugePoint("jetstream_consumer_num_waiting", "Number of in-flight fetch requests from a pull consumer", timestamp, float64(consumerInfo.NumWaiting), consumerLabels)
				builder.addGaugePoint("jetstream_consumer_num_pending", "Number of pending messages from a consumer", timestamp, float64(consumerInfo.NumPending), consumerLabels)
				builder.addGaugePoint("jetstream_consumer_ack_floor_stream_seq", "Number of ack floor stream seq from a consumer", timestamp, float64(consumerInfo.AckFloor.StreamSeq), consumerLabels)
				builder.addGaugePoint("jetstream_consumer_ack_floor_consumer_seq", "Number of ack floor consumer seq from a consumer", timestamp, float64(consumerInfo.AckFloor.ConsumerSeq), consumerLabels)
			}
		}
	}
}

func fqName(name string, prefix ...string) (string, []string) {
	path := make([]string, 0, len(prefix)+1)
	if len(prefix) > 0 {
		path = append(path, prefix...)
	}
	path = append(path, name)
	fqn := strings.Trim(strings.ReplaceAll(strings.Join(path, "_"), "/", "_"), "_")
	for strings.Contains(fqn, "__") {
		fqn = strings.ReplaceAll(fqn, "__", "_")
	}
	return fqn, path
}

func mergeLabels(base map[string]string, extra map[string]string) map[string]string {
	labels := make(map[string]string, len(base)+len(extra))
	for key, value := range base {
		labels[key] = value
	}
	for key, value := range extra {
		labels[key] = value
	}
	return labels
}

func joinEndpointPath(endpoint, path string) string {
	return strings.TrimRight(endpoint, "/") + path
}

func boolString(value bool) string {
	if value {
		return "true"
	}
	return "false"
}

func boolToFloat(value bool) float64 {
	if value {
		return 1
	}
	return 0
}

type jszResponse struct {
	ID             string             `json:"server_id"`
	Disabled       bool               `json:"disabled,omitempty"`
	Config         jszConfig          `json:"config,omitempty"`
	Streams        int                `json:"streams"`
	Consumers      int                `json:"consumers"`
	Messages       uint64             `json:"messages"`
	Bytes          uint64             `json:"bytes"`
	Meta           *jszMetaCluster    `json:"meta_cluster,omitempty"`
	AccountDetails []jszAccountDetail `json:"account_details,omitempty"`
}

type jszConfig struct {
	MaxMemory int64  `json:"max_memory"`
	MaxStore  int64  `json:"max_storage"`
	Domain    string `json:"domain,omitempty"`
}

type jszMetaCluster struct {
	Name   string `json:"name,omitempty"`
	Leader string `json:"leader,omitempty"`
}

type jszAccountDetail struct {
	Name           string            `json:"name"`
	ID             string            `json:"id"`
	Memory         uint64            `json:"memory"`
	Store          uint64            `json:"storage"`
	ReservedMemory uint64            `json:"reserved_memory"`
	ReservedStore  uint64            `json:"reserved_storage"`
	Streams        []jszStreamDetail `json:"stream_detail,omitempty"`
}

type jszStreamDetail struct {
	Name      string                 `json:"name"`
	Cluster   *jszClusterInfo        `json:"cluster,omitempty"`
	Config    *jszStreamConfig       `json:"config,omitempty"`
	State     jszStreamState         `json:"state,omitempty"`
	Consumers []*jszConsumerInfo     `json:"consumer_detail,omitempty"`
	Mirror    *jszStreamSourceInfo   `json:"mirror,omitempty"`
	Sources   []*jszStreamSourceInfo `json:"sources,omitempty"`
	RaftGroup string                 `json:"stream_raft_group,omitempty"`
}

type jszClusterInfo struct {
	Leader string `json:"leader,omitempty"`
}

type jszStreamConfig struct {
	MaxMsgs  int64 `json:"max_msgs"`
	MaxBytes int64 `json:"max_bytes"`
}

type jszStreamState struct {
	Messages      uint64 `json:"messages"`
	Bytes         uint64 `json:"bytes"`
	FirstSeq      uint64 `json:"first_seq"`
	LastSeq       uint64 `json:"last_seq"`
	ConsumerCount int    `json:"consumer_count"`
	SubjectCount  int    `json:"num_subjects"`
}

type jszConsumerInfo struct {
	Name           string             `json:"name"`
	Config         *jszConsumerConfig `json:"config,omitempty"`
	Delivered      jszSequenceInfo    `json:"delivered"`
	AckFloor       jszSequenceInfo    `json:"ack_floor"`
	NumAckPending  int                `json:"num_ack_pending"`
	NumRedelivered int                `json:"num_redelivered"`
	NumWaiting     int                `json:"num_waiting"`
	NumPending     uint64             `json:"num_pending"`
	Cluster        *jszClusterInfo    `json:"cluster,omitempty"`
}

type jszConsumerConfig struct {
	Description string `json:"description,omitempty"`
}

type jszSequenceInfo struct {
	ConsumerSeq uint64 `json:"consumer_seq"`
	StreamSeq   uint64 `json:"stream_seq"`
}

type jszStreamSourceInfo struct {
	Name     string             `json:"name"`
	External *jszExternalStream `json:"external,omitempty"`
	Lag      uint64             `json:"lag"`
	Active   time.Duration      `json:"active"`
}

type jszExternalStream struct {
	APIPrefix     string `json:"api"`
	DeliverPrefix string `json:"deliver"`
}

func (info *jszStreamSourceInfo) externalAPI() string {
	if info == nil || info.External == nil {
		return ""
	}
	return info.External.APIPrefix
}

func (info *jszStreamSourceInfo) externalDeliver() string {
	if info == nil || info.External == nil {
		return ""
	}
	return info.External.DeliverPrefix
}
