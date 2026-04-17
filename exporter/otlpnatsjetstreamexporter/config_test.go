package otlpnatsjetstreamexporter

import (
	"testing"

	"go.opentelemetry.io/collector/confmap"
)

func TestConfigUnmarshalAcceptsComponentFields(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	conf := confmap.NewFromStringMap(map[string]any{
		"endpoint":       "nats://127.0.0.1:4222",
		"subject_prefix": "demo",
		"headers": map[string]any{
			"x-test": "value",
		},
	})

	if err := conf.Unmarshal(cfg); err != nil {
		t.Fatalf("unmarshal exporter config: %v", err)
	}

	if len(cfg.Endpoints) != 1 || cfg.Endpoints[0] != "nats://127.0.0.1:4222" {
		t.Fatalf("unexpected endpoints: %#v", cfg.Endpoints)
	}
	if cfg.SubjectPrefix != "demo" {
		t.Fatalf("unexpected subject prefix: %q", cfg.SubjectPrefix)
	}
	if cfg.Headers["x-test"] != "value" {
		t.Fatalf("unexpected headers: %#v", cfg.Headers)
	}
	if cfg.TimeoutConfig.Timeout <= 0 {
		t.Fatalf("expected default timeout to be preserved, got %v", cfg.TimeoutConfig.Timeout)
	}
}