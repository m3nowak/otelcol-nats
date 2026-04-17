package natsbrokerreceiver

import (
	"context"

	"github.com/m3nowak/otelcol-nats/receiver/natsbrokerreceiver/internal/metadata"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
)

func NewFactory() receiver.Factory {
	return receiver.NewFactory(
		metadata.Type,
		createDefaultConfig,
		receiver.WithMetrics(createMetrics, metadata.MetricsStability),
	)
}

func createMetrics(_ context.Context, set receiver.Settings, cfg component.Config, next consumer.Metrics) (receiver.Metrics, error) {
	return newReceiver(cfg.(*Config), set, next)
}
