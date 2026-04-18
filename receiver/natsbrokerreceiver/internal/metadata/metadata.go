package metadata

import "go.opentelemetry.io/collector/component"

var Type = component.MustNewType("nats_broker")

const MetricsStability = component.StabilityLevelAlpha
