package natsjetstream

import "testing"

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