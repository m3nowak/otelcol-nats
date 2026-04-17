package natsbrokerreceiver

import (
	"testing"
	"time"

	"go.opentelemetry.io/collector/confmap"
)

func TestConfigUnmarshalAcceptsStringAndAuth(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	conf := confmap.NewFromStringMap(map[string]any{
		"endpoint": "https://nats.example.com:8222",
		"auth": map[string]any{
			"username": "demo",
			"password": "secret",
		},
		"collection_interval": "30s",
		"timeout":             "7s",
	})

	if err := conf.Unmarshal(cfg); err != nil {
		t.Fatalf("unmarshal config: %v", err)
	}

	if len(cfg.Endpoints) != 1 || cfg.Endpoints[0] != "https://nats.example.com:8222" {
		t.Fatalf("unexpected endpoints: %#v", cfg.Endpoints)
	}
	if cfg.Auth.Username != "demo" || string(cfg.Auth.Password) != "secret" {
		t.Fatalf("unexpected auth config: %#v", cfg.Auth)
	}
	if cfg.CollectionInterval != 30*time.Second {
		t.Fatalf("unexpected collection interval: %v", cfg.CollectionInterval)
	}
	if cfg.Timeout != 7*time.Second {
		t.Fatalf("unexpected timeout: %v", cfg.Timeout)
	}
}

func TestConfigValidateRejectsInvalidSettings(t *testing.T) {
	testCases := []struct {
		name string
		cfg  Config
	}{
		{
			name: "invalid scheme",
			cfg: Config{
				Endpoints:          []string{"nats://127.0.0.1:8222"},
				CollectionInterval: 10 * time.Second,
				Timeout:            5 * time.Second,
			},
		},
		{
			name: "missing auth password",
			cfg: Config{
				Endpoints:          []string{"http://127.0.0.1:8222"},
				Auth:               AuthConfig{Username: "demo"},
				CollectionInterval: 10 * time.Second,
				Timeout:            5 * time.Second,
			},
		},
		{
			name: "zero interval",
			cfg: Config{
				Endpoints: []string{"http://127.0.0.1:8222"},
				Timeout:   5 * time.Second,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.cfg.Validate(); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}
