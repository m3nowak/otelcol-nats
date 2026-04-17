package natsjetstream

import (
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	"go.opentelemetry.io/collector/config/configcompression"
	"go.opentelemetry.io/collector/config/configopaque"
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
	cfg.Auth.JWT = "test-jwt-token"
	cfg.Auth.NKey = mustCreateUserSeed(t)

	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected jwt+nkey auth to be valid, got %v", err)
	}
}

func TestClientConfigRejectsNKeyCombinedWithToken(t *testing.T) {
	cfg := NewDefaultClientConfig()
	cfg.Auth.Token = "token"
	cfg.Auth.NKey = mustCreateUserSeed(t)

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected nkey+token auth to be rejected")
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
	seed, publicKey := mustCreateUserCredentials(t)
	cfg := NewDefaultClientConfig()
	cfg.Auth.JWT = "jwt-with-seed"
	cfg.Auth.NKey = seed

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
	verifier, err := nkeys.FromPublicKey(publicKey)
	if err != nil {
		t.Fatalf("load public nkey: %v", err)
	}
	if err := verifier.Verify([]byte("nonce"), signature); err != nil {
		t.Fatalf("verify nonce signature: %v", err)
	}
}

func mustCreateUserSeed(t *testing.T) configopaque.String {
	t.Helper()

	keyPair, err := nkeys.CreateUser()
	if err != nil {
		t.Fatalf("create user nkey: %v", err)
	}
	seed, _ := mustCreateUserCredentialsFromPair(t, keyPair)
	return seed
}

func mustCreateUserCredentials(t *testing.T) (configopaque.String, string) {
	t.Helper()

	keyPair, err := nkeys.CreateUser()
	if err != nil {
		t.Fatalf("create user nkey: %v", err)
	}
	return mustCreateUserCredentialsFromPair(t, keyPair)
}

func mustCreateUserCredentialsFromPair(t *testing.T, keyPair nkeys.KeyPair) (configopaque.String, string) {
	t.Helper()

	seed, err := keyPair.Seed()
	if err != nil {
		t.Fatalf("export user seed: %v", err)
	}
	publicKey, err := keyPair.PublicKey()
	if err != nil {
		t.Fatalf("export user public key: %v", err)
	}
	return configopaque.String(seed), publicKey
}
