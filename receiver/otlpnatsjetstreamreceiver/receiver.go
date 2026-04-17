package otlpnatsjetstreamreceiver

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/m3nowak/otelcol-nats/internal/natsjetstream"
	"github.com/m3nowak/otelcol-nats/receiver/otlpnatsjetstreamreceiver/internal/metadata"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/receiverhelper"
	"go.uber.org/zap"
)

var receiverRegistry = struct {
	mu    sync.Mutex
	items map[*Config]*receiverComponent
}{
	items: map[*Config]*receiverComponent{},
}

type receiverComponent struct {
	cfg *Config
	set receiver.Settings

	mu sync.RWMutex

	nextTraces  consumer.Traces
	nextMetrics consumer.Metrics
	nextLogs    consumer.Logs
	obsreport   *receiverhelper.ObsReport

	started          bool
	conn             *nats.Conn
	js               jetstream.JetStream
	consumer         jetstream.Consumer
	consumeCtx       jetstream.ConsumeContext
	streamName       string
	consumerName     string
	createdEphemeral bool
}

func getOrCreateReceiver(cfg *Config, set receiver.Settings) (*receiverComponent, error) {
	receiverRegistry.mu.Lock()
	defer receiverRegistry.mu.Unlock()

	if existing := receiverRegistry.items[cfg]; existing != nil {
		return existing, nil
	}

	obsreport, err := receiverhelper.NewObsReport(receiverhelper.ObsReportSettings{
		ReceiverID:             component.NewID(metadata.Type),
		Transport:              "nats_jetstream",
		ReceiverCreateSettings: set,
	})
	if err != nil {
		return nil, err
	}

	rcv := &receiverComponent{
		cfg:       cfg,
		set:       set,
		obsreport: obsreport,
	}
	receiverRegistry.items[cfg] = rcv
	return rcv, nil
}

func (rcv *receiverComponent) registerTracesConsumer(next consumer.Traces) {
	rcv.mu.Lock()
	defer rcv.mu.Unlock()
	rcv.nextTraces = next
}

func (rcv *receiverComponent) registerMetricsConsumer(next consumer.Metrics) {
	rcv.mu.Lock()
	defer rcv.mu.Unlock()
	rcv.nextMetrics = next
}

func (rcv *receiverComponent) registerLogsConsumer(next consumer.Logs) {
	rcv.mu.Lock()
	defer rcv.mu.Unlock()
	rcv.nextLogs = next
}

func (rcv *receiverComponent) Start(ctx context.Context, _ component.Host) error {
	rcv.mu.Lock()
	if rcv.started {
		rcv.mu.Unlock()
		return nil
	}
	activeSignals := rcv.activeSignalsLocked()
	rcv.mu.Unlock()

	if len(activeSignals) == 0 {
		return errors.New("receiver has no registered downstream consumers")
	}

	conn, js, err := natsjetstream.Connect(ctx, rcv.cfg.ClientConfig)
	if err != nil {
		return err
	}

	streamName, err := rcv.resolveStreamName(ctx, js, activeSignals)
	if err != nil {
		conn.Close()
		return err
	}

	consumerInstance, consumerName, createdEphemeral, err := rcv.resolveConsumer(ctx, js, streamName, activeSignals)
	if err != nil {
		conn.Close()
		return err
	}

	consumeCtx, err := consumerInstance.Consume(
		func(msg jetstream.Msg) {
			rcv.handleMessage(msg)
		},
		jetstream.PullExpiry(maxDuration(time.Second, rcv.cfg.Timeout)),
	)
	if err != nil {
		if createdEphemeral {
			_ = js.DeleteConsumer(context.Background(), streamName, consumerName)
		}
		conn.Close()
		return fmt.Errorf("start consumer loop: %w", err)
	}

	rcv.mu.Lock()
	rcv.conn = conn
	rcv.js = js
	rcv.streamName = streamName
	rcv.consumer = consumerInstance
	rcv.consumerName = consumerName
	rcv.createdEphemeral = createdEphemeral
	rcv.consumeCtx = consumeCtx
	rcv.started = true
	rcv.mu.Unlock()

	rcv.set.Logger.Info(
		"connected OTLP NATS JetStream receiver",
		zap.Strings("endpoints", rcv.cfg.Endpoints),
		zap.String("stream", streamName),
		zap.String("consumer", consumerName),
	)

	return nil
}

func (rcv *receiverComponent) Shutdown(ctx context.Context) error {
	rcv.mu.Lock()
	consumeCtx := rcv.consumeCtx
	conn := rcv.conn
	js := rcv.js
	streamName := rcv.streamName
	consumerName := rcv.consumerName
	createdEphemeral := rcv.createdEphemeral
	wasStarted := rcv.started
	rcv.consumeCtx = nil
	rcv.consumer = nil
	rcv.conn = nil
	rcv.js = nil
	rcv.streamName = ""
	rcv.consumerName = ""
	rcv.createdEphemeral = false
	rcv.started = false
	rcv.mu.Unlock()

	if !wasStarted {
		removeReceiver(rcv.cfg)
		return nil
	}

	var shutdownErr error
	if consumeCtx != nil {
		consumeCtx.Drain()
		select {
		case <-consumeCtx.Closed():
		case <-ctx.Done():
			shutdownErr = ctx.Err()
		}
	}

	if createdEphemeral && js != nil && streamName != "" && consumerName != "" {
		if err := js.DeleteConsumer(context.Background(), streamName, consumerName); err != nil && !errors.Is(err, jetstream.ErrConsumerNotFound) {
			shutdownErr = errors.Join(shutdownErr, err)
		}
	}

	if conn != nil {
		_ = conn.Drain()
		conn.Close()
	}

	removeReceiver(rcv.cfg)
	return shutdownErr
}

func (rcv *receiverComponent) activeSignalsLocked() []natsjetstream.Signal {
	activeSignals := make([]natsjetstream.Signal, 0, 3)
	if rcv.nextTraces != nil {
		activeSignals = append(activeSignals, natsjetstream.SignalTraces)
	}
	if rcv.nextMetrics != nil {
		activeSignals = append(activeSignals, natsjetstream.SignalMetrics)
	}
	if rcv.nextLogs != nil {
		activeSignals = append(activeSignals, natsjetstream.SignalLogs)
	}
	return activeSignals
}

func (rcv *receiverComponent) activeSignals() []natsjetstream.Signal {
	rcv.mu.RLock()
	defer rcv.mu.RUnlock()
	return rcv.activeSignalsLocked()
}

func (rcv *receiverComponent) resolveStreamName(ctx context.Context, js jetstream.JetStream, activeSignals []natsjetstream.Signal) (string, error) {
	if rcv.cfg.Stream.Name != "" {
		if _, err := js.Stream(ctx, rcv.cfg.Stream.Name); err != nil {
			return "", fmt.Errorf("open stream %q: %w", rcv.cfg.Stream.Name, err)
		}
		return rcv.cfg.Stream.Name, nil
	}

	resolvedStream := ""
	for _, signal := range activeSignals {
		streamName, err := js.StreamNameBySubject(ctx, natsjetstream.Subject(rcv.cfg.subjectPrefix(), signal))
		if err != nil {
			return "", fmt.Errorf("autodiscover stream for %s: %w", signal, err)
		}
		if resolvedStream == "" {
			resolvedStream = streamName
			continue
		}
		if resolvedStream != streamName {
			return "", fmt.Errorf("autodiscovery resolved multiple streams: %q and %q", resolvedStream, streamName)
		}
	}

	if resolvedStream == "" {
		return "", errors.New("autodiscovery did not resolve any stream")
	}
	return resolvedStream, nil
}

func (rcv *receiverComponent) resolveConsumer(ctx context.Context, js jetstream.JetStream, streamName string, activeSignals []natsjetstream.Signal) (jetstream.Consumer, string, bool, error) {
	if durable := strings.TrimSpace(rcv.cfg.Consumer.Durable); durable != "" {
		consumerInstance, err := js.Consumer(ctx, streamName, durable)
		if err != nil {
			return nil, "", false, fmt.Errorf("open durable consumer %q: %w", durable, err)
		}
		return consumerInstance, durable, false, nil
	}

	consumerConfig := jetstream.ConsumerConfig{
		Name:              strings.TrimSpace(rcv.cfg.Consumer.Ephemeral),
		DeliverPolicy:     jetstream.DeliverNewPolicy,
		AckPolicy:         jetstream.AckExplicitPolicy,
		AckWait:           maxDuration(30*time.Second, rcv.cfg.Timeout*2),
		MaxWaiting:        1,
		InactiveThreshold: maxDuration(30*time.Second, rcv.cfg.Timeout*3),
	}

	filterSubjects := rcv.filterSubjects(activeSignals)
	if len(filterSubjects) == 1 {
		consumerConfig.FilterSubject = filterSubjects[0]
	} else {
		consumerConfig.FilterSubjects = filterSubjects
	}

	consumerInstance, err := js.CreateOrUpdateConsumer(ctx, streamName, consumerConfig)
	if err != nil {
		return nil, "", false, fmt.Errorf("create ephemeral consumer: %w", err)
	}

	info, err := consumerInstance.Info(ctx)
	if err != nil {
		return nil, "", false, fmt.Errorf("inspect ephemeral consumer: %w", err)
	}

	return consumerInstance, info.Name, true, nil
}

func (rcv *receiverComponent) filterSubjects(activeSignals []natsjetstream.Signal) []string {
	filterSubjects := make([]string, 0, len(activeSignals))
	for _, signal := range activeSignals {
		filterSubjects = append(filterSubjects, natsjetstream.Subject(rcv.cfg.subjectPrefix(), signal))
	}
	slices.Sort(filterSubjects)
	return filterSubjects
}

func (rcv *receiverComponent) handleMessage(msg jetstream.Msg) {
	action, err := rcv.consumeMessage(msg)
	if err != nil {
		rcv.set.Logger.Error("failed to process JetStream message", zap.String("subject", msg.Subject()), zap.Error(err))
	}

	switch action {
	case ackActionAck:
		if err := msg.Ack(); err != nil {
			rcv.set.Logger.Error("failed to ack JetStream message", zap.Error(err))
		}
	case ackActionNak:
		if err := msg.Nak(); err != nil {
			rcv.set.Logger.Error("failed to nak JetStream message", zap.Error(err))
		}
	case ackActionTerm:
		if err := msg.Term(); err != nil {
			rcv.set.Logger.Error("failed to term JetStream message", zap.Error(err))
		}
	}
}

func (rcv *receiverComponent) consumeMessage(msg jetstream.Msg) (ackAction, error) {
	signal, ok := natsjetstream.SignalFromSubject(msg.Subject())
	if !ok {
		return ackActionTerm, fmt.Errorf("unsupported subject %q", msg.Subject())
	}

	compression := msg.Headers().Get("Content-Encoding")
	payload, err := natsjetstream.DecompressPayload(msg.Data(), compression)
	if err != nil {
		return ackActionTerm, err
	}

	switch signal {
	case natsjetstream.SignalTraces:
		return rcv.consumeTraces(payload)
	case natsjetstream.SignalMetrics:
		return rcv.consumeMetrics(payload)
	case natsjetstream.SignalLogs:
		return rcv.consumeLogs(payload)
	default:
		return ackActionTerm, fmt.Errorf("unsupported signal %q", signal)
	}
}

func (rcv *receiverComponent) consumeTraces(payload []byte) (ackAction, error) {
	rcv.mu.RLock()
	next := rcv.nextTraces
	rcv.mu.RUnlock()
	if next == nil {
		return ackActionAck, nil
	}

	request := ptraceotlp.NewExportRequest()
	if err := request.UnmarshalProto(payload); err != nil {
		ctx := rcv.obsreport.StartTracesOp(context.Background())
		rcv.obsreport.EndTracesOp(ctx, "protobuf", 0, err)
		return ackActionTerm, err
	}

	traces := request.Traces()
	ctx := rcv.obsreport.StartTracesOp(context.Background())
	err := next.ConsumeTraces(ctx, traces)
	rcv.obsreport.EndTracesOp(ctx, "protobuf", traces.SpanCount(), err)
	if err != nil {
		return ackActionNak, err
	}
	return ackActionAck, nil
}

func (rcv *receiverComponent) consumeMetrics(payload []byte) (ackAction, error) {
	rcv.mu.RLock()
	next := rcv.nextMetrics
	rcv.mu.RUnlock()
	if next == nil {
		return ackActionAck, nil
	}

	request := pmetricotlp.NewExportRequest()
	if err := request.UnmarshalProto(payload); err != nil {
		ctx := rcv.obsreport.StartMetricsOp(context.Background())
		rcv.obsreport.EndMetricsOp(ctx, "protobuf", 0, err)
		return ackActionTerm, err
	}

	metrics := request.Metrics()
	ctx := rcv.obsreport.StartMetricsOp(context.Background())
	err := next.ConsumeMetrics(ctx, metrics)
	rcv.obsreport.EndMetricsOp(ctx, "protobuf", metrics.DataPointCount(), err)
	if err != nil {
		return ackActionNak, err
	}
	return ackActionAck, nil
}

func (rcv *receiverComponent) consumeLogs(payload []byte) (ackAction, error) {
	rcv.mu.RLock()
	next := rcv.nextLogs
	rcv.mu.RUnlock()
	if next == nil {
		return ackActionAck, nil
	}

	request := plogotlp.NewExportRequest()
	if err := request.UnmarshalProto(payload); err != nil {
		ctx := rcv.obsreport.StartLogsOp(context.Background())
		rcv.obsreport.EndLogsOp(ctx, "protobuf", 0, err)
		return ackActionTerm, err
	}

	logs := request.Logs()
	ctx := rcv.obsreport.StartLogsOp(context.Background())
	err := next.ConsumeLogs(ctx, logs)
	rcv.obsreport.EndLogsOp(ctx, "protobuf", logs.LogRecordCount(), err)
	if err != nil {
		return ackActionNak, err
	}
	return ackActionAck, nil
}

func removeReceiver(cfg *Config) {
	receiverRegistry.mu.Lock()
	defer receiverRegistry.mu.Unlock()
	delete(receiverRegistry.items, cfg)
}

type ackAction int

const (
	ackActionAck ackAction = iota
	ackActionNak
	ackActionTerm
)

func maxDuration(left, right time.Duration) time.Duration {
	if left > right {
		return left
	}
	return right
}
