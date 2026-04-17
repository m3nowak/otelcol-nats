package natsjetstream

import (
	"bytes"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	"go.opentelemetry.io/collector/config/configcompression"
)

func TestClientConfigRejectsMixedEndpointSchemes(t *testing.T) {
	cfg := NewDefaultClientConfig()
	cfg.Endpoints = []string{"nats://127.0.0.1:4222", "ws://127.0.0.1:4223"}

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected mixed endpoint validation error")
	}
}

func TestSubjectUsesDefaultPrefix(t *testing.T) {
	got := Subject("", SignalTraces)
	if got != "otlp.v1.traces" {
		t.Fatalf("unexpected subject: %s", got)
	}
}

func TestClientConfigRejectsInvalidCompressionParams(t *testing.T) {
	cfg := NewDefaultClientConfig()
	cfg.Compression = configcompression.TypeGzip
	cfg.CompressionParams.Level = 123

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected compression validation error")
	}
}

func TestClientConfigAllowsJWTWithNKey(t *testing.T) {
	cfg := NewDefaultClientConfig()
	cfg.Auth.JWT = "eyJhbGciOiJIUzI1NiJ9.eyJuYXRzIjp7fX0.signature"
	cfg.Auth.NKey = mustCreateUserSeed(t)

	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected jwt+nkey auth to be valid, got %v", err)
	}
}

func TestBuildConnectionOptionsUsesBearerJWTWhenSeedIsMissing(t *testing.T) {
	cfg := NewDefaultClientConfig()
	cfg.Auth.JWT = "jwt-bearer-token"

	options, err := buildConnectionOptions(cfg)
	if err != nil {
		t.Fatalf("build connection options: %v", err)
	}

	natsOptions := nats.GetDefaultOptions()
	for _, option := range options {
		if err := option(&natsOptions); err != nil {
			t.Fatalf("apply option: %v", err)
		}
	}

	if natsOptions.User != "bearer" {
		t.Fatalf("unexpected bearer user: %q", natsOptions.User)
	}
	if natsOptions.Password != "jwt-bearer-token" {
		t.Fatalf("unexpected bearer password: %q", natsOptions.Password)
	}
}

func TestBuildConnectionOptionsUsesUserJWTAndSeedWhenBothConfigured(t *testing.T) {
	cfg := NewDefaultClientConfig()
	cfg.Auth.JWT = "jwt-with-seed"
	cfg.Auth.NKey = mustCreateUserSeed(t)

	options, err := buildConnectionOptions(cfg)
	if err != nil {
		t.Fatalf("build connection options: %v", err)
	}

	natsOptions := nats.GetDefaultOptions()
	for _, option := range options {
		if err := option(&natsOptions); err != nil {
			t.Fatalf("apply option: %v", err)
		}
	}

	if natsOptions.User != "" || natsOptions.Password != "" {
		t.Fatalf("expected jwt auth instead of user/password, got user=%q password=%q", natsOptions.User, natsOptions.Password)
	}
	if natsOptions.UserJWT == nil {
		t.Fatal("expected jwt callback to be configured")
	}
	if natsOptions.SignatureCB == nil {
		t.Fatal("expected signature callback to be configured")
	}

	jwt, err := natsOptions.UserJWT()
	if err != nil {
		t.Fatalf("read jwt callback: %v", err)
	}
	if jwt != "jwt-with-seed" {
		t.Fatalf("unexpected jwt: %q", jwt)
	}

	signature, err := natsOptions.SignatureCB([]byte("nonce"))
	if err != nil {
		t.Fatalf("sign nonce: %v", err)
	}
	if len(signature) == 0 {
		t.Fatal("expected nonce signature")
	}
	if bytes.Equal(signature, []byte("nonce")) {
		t.Fatal("expected nonce signature to differ from the nonce")
	}
}

func mustCreateUserSeed(t *testing.T) string {
	t.Helper()

	keyPair, err := nkeys.CreateUser()
	if err != nil {
		t.Fatalf("create user nkey: %v", err)
	}
	seed, err := keyPair.Seed()
	if err != nil {
		t.Fatalf("export user seed: %v", err)
	}
	return string(seed)
}
