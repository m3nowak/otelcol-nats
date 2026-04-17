package otlpnatsjetstreamexporter

import (
	"context"

	"github.com/m3nowak/otelcol-nats/exporter/otlpnatsjetstreamexporter/internal/metadata"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		metadata.Type,
		createDefaultConfig,
		exporter.WithTraces(createTraces, metadata.TracesStability),
		exporter.WithMetrics(createMetrics, metadata.MetricsStability),
		exporter.WithLogs(createLogs, metadata.LogsStability),
	)
}

func createTraces(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Traces, error) {
	component := newExporter(cfg, set)
	config := cfg.(*Config)
	return exporterhelper.NewTraces(
		ctx,
		set,
		cfg,
		component.pushTraces,
		exporterhelper.WithCapabilities(consumer.Capabilities{MutatesData: false}),
		exporterhelper.WithTimeout(config.TimeoutConfig),
		exporterhelper.WithRetry(config.RetryConfig),
		exporterhelper.WithQueue(config.QueueConfig),
		exporterhelper.WithStart(component.start),
		exporterhelper.WithShutdown(component.shutdown),
	)
}

func createMetrics(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Metrics, error) {
	component := newExporter(cfg, set)
	config := cfg.(*Config)
	return exporterhelper.NewMetrics(
		ctx,
		set,
		cfg,
		component.pushMetrics,
		exporterhelper.WithCapabilities(consumer.Capabilities{MutatesData: false}),
		exporterhelper.WithTimeout(config.TimeoutConfig),
		exporterhelper.WithRetry(config.RetryConfig),
		exporterhelper.WithQueue(config.QueueConfig),
		exporterhelper.WithStart(component.start),
		exporterhelper.WithShutdown(component.shutdown),
	)
}

func createLogs(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Logs, error) {
	component := newExporter(cfg, set)
	config := cfg.(*Config)
	return exporterhelper.NewLogs(
		ctx,
		set,
		cfg,
		component.pushLogs,
		exporterhelper.WithCapabilities(consumer.Capabilities{MutatesData: false}),
		exporterhelper.WithTimeout(config.TimeoutConfig),
		exporterhelper.WithRetry(config.RetryConfig),
		exporterhelper.WithQueue(config.QueueConfig),
		exporterhelper.WithStart(component.start),
		exporterhelper.WithShutdown(component.shutdown),
	)
}