package metadata

import (
	"go.opentelemetry.io/collector/component"
)

var Type = component.MustNewType("otlp_nats_jetstream")

const (
	TracesStability  = component.StabilityLevelAlpha
	MetricsStability = component.StabilityLevelAlpha
	LogsStability    = component.StabilityLevelAlpha
)