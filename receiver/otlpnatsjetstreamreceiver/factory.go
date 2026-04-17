package otlpnatsjetstreamreceiver

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
	"github.com/m3nowak/otelcol-nats/receiver/otlpnatsjetstreamreceiver/internal/metadata"
)

func NewFactory() receiver.Factory {
	return receiver.NewFactory(
		metadata.Type,
		createDefaultConfig,
		receiver.WithTraces(createTraces, metadata.TracesStability),
		receiver.WithMetrics(createMetrics, metadata.MetricsStability),
		receiver.WithLogs(createLogs, metadata.LogsStability),
	)
}

func createTraces(_ context.Context, set receiver.Settings, cfg component.Config, next consumer.Traces) (receiver.Traces, error) {
	rcv, err := getOrCreateReceiver(cfg.(*Config), set)
	if err != nil {
		return nil, err
	}
	rcv.registerTracesConsumer(next)
	return rcv, nil
}

func createMetrics(_ context.Context, set receiver.Settings, cfg component.Config, next consumer.Metrics) (receiver.Metrics, error) {
	rcv, err := getOrCreateReceiver(cfg.(*Config), set)
	if err != nil {
		return nil, err
	}
	rcv.registerMetricsConsumer(next)
	return rcv, nil
}

func createLogs(_ context.Context, set receiver.Settings, cfg component.Config, next consumer.Logs) (receiver.Logs, error) {
	rcv, err := getOrCreateReceiver(cfg.(*Config), set)
	if err != nil {
		return nil, err
	}
	rcv.registerLogsConsumer(next)
	return rcv, nil
}