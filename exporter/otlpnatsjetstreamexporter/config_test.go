package otlpnatsjetstreamexporter

import (
	"testing"

	"go.opentelemetry.io/collector/confmap"
)

func TestConfigUnmarshalAcceptsComponentFields(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	conf := confmap.NewFromStringMap(map[string]any{
		"endpoint":        "nats://127.0.0.1:4222",
		"subject_prefix":  "demo",
		"expected_stream": "OTLP",
		"compression":     "zstd",
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
	if cfg.ExpectedStream != "OTLP" {
		t.Fatalf("unexpected expected stream: %q", cfg.ExpectedStream)
	}
	if cfg.Compression != "zstd" {
		t.Fatalf("unexpected compression: %q", cfg.Compression)
	}
	if cfg.Headers["x-test"] != "value" {
		t.Fatalf("unexpected headers: %#v", cfg.Headers)
	}
	if cfg.TimeoutConfig.Timeout <= 0 {
		t.Fatalf("expected default timeout to be preserved, got %v", cfg.TimeoutConfig.Timeout)
	}
}

func TestCreateDefaultConfigUsesGzipCompression(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	if cfg.Compression != "gzip" {
		t.Fatalf("unexpected default compression: %q", cfg.Compression)
	}
	if cfg.ExpectedStream != "" {
		t.Fatalf("unexpected default expected stream: %q", cfg.ExpectedStream)
	}
}

func TestConfigValidateNormalizesSubjectPrefixAndExpectedStream(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.SubjectPrefix = " demo. "
	cfg.ExpectedStream = " OTLP "

	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate config: %v", err)
	}
	if cfg.SubjectPrefix != "demo" {
		t.Fatalf("unexpected normalized subject prefix: %q", cfg.SubjectPrefix)
	}
	if cfg.ExpectedStream != "OTLP" {
		t.Fatalf("unexpected normalized expected stream: %q", cfg.ExpectedStream)
	}
}

func TestConfigValidateRejectsBlankSubjectPrefix(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.SubjectPrefix = " . "

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected validation error for blank subject prefix")
	}
	if err.Error() != `requires a non-empty "subject_prefix"` {
		t.Fatalf("unexpected validation error: %v", err)
	}
}
