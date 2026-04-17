package otlpnatsjetstreamreceiver

import (
	"testing"
	"time"

	"github.com/m3nowak/otelcol-nats/internal/natsjetstream"
	"go.opentelemetry.io/collector/confmap"
)

func TestConfigUnmarshalAcceptsReceiverFields(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	conf := confmap.NewFromStringMap(map[string]any{
		"endpoint": "nats://127.0.0.1:4222",
		"stream": map[string]any{
			"name": "otlp",
		},
		"consumer": map[string]any{
			"durable": "otlp-consumer",
		},
	})

	if err := conf.Unmarshal(cfg); err != nil {
		t.Fatalf("unmarshal receiver config: %v", err)
	}

	if len(cfg.Endpoints) != 1 || cfg.Endpoints[0] != "nats://127.0.0.1:4222" {
		t.Fatalf("unexpected endpoints: %#v", cfg.Endpoints)
	}
	if cfg.Stream.Name != "otlp" {
		t.Fatalf("unexpected stream name: %q", cfg.Stream.Name)
	}
	if cfg.Consumer.Durable != "otlp-consumer" {
		t.Fatalf("unexpected durable consumer: %q", cfg.Consumer.Durable)
	}
	if cfg.Stream.AutodiscoverPrefix != natsjetstream.DefaultSubjectPrefix+".>" {
		t.Fatalf("expected default autodiscover prefix to be preserved, got %q", cfg.Stream.AutodiscoverPrefix)
	}
	if cfg.Timeout != 5*time.Second {
		t.Fatalf("expected default timeout to be preserved, got %v", cfg.Timeout)
	}
}

func TestConfigUnmarshalRejectsRemovedCompressionFields(t *testing.T) {
	testCases := []struct {
		name string
		data map[string]any
	}{
		{
			name: "compression",
			data: map[string]any{
				"compression": "gzip",
			},
		},
		{
			name: "compression params",
			data: map[string]any{
				"compression_params": map[string]any{
					"level": 1,
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := createDefaultConfig().(*Config)
			conf := confmap.NewFromStringMap(tc.data)

			if err := conf.Unmarshal(cfg); err == nil {
				t.Fatal("expected unmarshal error")
			}
		})
	}
}
