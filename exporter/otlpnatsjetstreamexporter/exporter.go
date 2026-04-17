package otlpnatsjetstreamexporter

import (
	"context"
	"fmt"
	"sync"

	"github.com/m3nowak/otelcol-nats/internal/natsjetstream"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"go.uber.org/zap"
)

type exporterComponent struct {
	cfg *Config
	set exporter.Settings

	mu   sync.RWMutex
	conn *nats.Conn
	js   jetstream.JetStream
}

func newExporter(cfg component.Config, set exporter.Settings) *exporterComponent {
	return &exporterComponent{cfg: cfg.(*Config), set: set}
}

func (exp *exporterComponent) start(ctx context.Context, _ component.Host) error {
	conn, js, err := natsjetstream.Connect(ctx, exp.cfg.ClientConfig)
	if err != nil {
		return err
	}

	exp.mu.Lock()
	exp.conn = conn
	exp.js = js
	exp.mu.Unlock()

	exp.set.Logger.Info("connected OTLP NATS JetStream exporter", zap.Strings("endpoints", exp.cfg.Endpoints))
	return nil
}

func (exp *exporterComponent) shutdown(context.Context) error {
	exp.mu.Lock()
	defer exp.mu.Unlock()
	if exp.conn != nil {
		exp.conn.Drain()
		exp.conn.Close()
		exp.conn = nil
		exp.js = nil
	}
	return nil
}

func (exp *exporterComponent) pushTraces(ctx context.Context, traces ptrace.Traces) error {
	request := ptraceotlp.NewExportRequestFromTraces(traces)
	payload, err := request.MarshalProto()
	if err != nil {
		return fmt.Errorf("marshal traces: %w", err)
	}
	return exp.publish(ctx, natsjetstream.SignalTraces, payload)
}

func (exp *exporterComponent) pushMetrics(ctx context.Context, metrics pmetric.Metrics) error {
	request := pmetricotlp.NewExportRequestFromMetrics(metrics)
	payload, err := request.MarshalProto()
	if err != nil {
		return fmt.Errorf("marshal metrics: %w", err)
	}
	return exp.publish(ctx, natsjetstream.SignalMetrics, payload)
}

func (exp *exporterComponent) pushLogs(ctx context.Context, logs plog.Logs) error {
	request := plogotlp.NewExportRequestFromLogs(logs)
	payload, err := request.MarshalProto()
	if err != nil {
		return fmt.Errorf("marshal logs: %w", err)
	}
	return exp.publish(ctx, natsjetstream.SignalLogs, payload)
}

func (exp *exporterComponent) publish(ctx context.Context, signal natsjetstream.Signal, payload []byte) error {
	exp.mu.RLock()
	js := exp.js
	exp.mu.RUnlock()
	if js == nil {
		return fmt.Errorf("exporter is not started")
	}

	compressedPayload, err := natsjetstream.CompressPayload(payload, exp.cfg.Compression, exp.cfg.CompressionParams)
	if err != nil {
		return err
	}

	message := nats.NewMsg(natsjetstream.Subject(exp.cfg.SubjectPrefix, signal))
	message.Data = compressedPayload
	for key, value := range exp.cfg.Headers {
		message.Header.Set(key, value)
	}
	if exp.cfg.Compression.IsCompressed() {
		message.Header.Set("Content-Encoding", string(exp.cfg.Compression))
	}

	if _, err := js.PublishMsg(ctx, message); err != nil {
		return fmt.Errorf("publish %s: %w", signal, err)
	}

	return nil
}
