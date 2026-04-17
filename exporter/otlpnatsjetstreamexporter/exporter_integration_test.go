package otlpnatsjetstreamexporter

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/m3nowak/otelcol-nats/internal/natsjetstream"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configcompression"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"go.uber.org/zap"
)

func TestExporterPublishesTracePayloadToJetStream(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	clientCfg := natsjetstream.NewDefaultClientConfig()
	clientCfg.Endpoints = []string{natsjetstream.DefaultEndpoint}

	conn, js, err := natsjetstream.Connect(ctx, clientCfg)
	if err != nil {
		if errors.Is(err, nats.ErrNoServers) {
			t.Skip("local NATS server is not available")
		}
		t.Fatalf("connect to local NATS: %v", err)
	}
	defer conn.Close()

	prefix := fmt.Sprintf("itest%d", time.Now().UnixNano())
	streamName := fmt.Sprintf("ITEST_%d", time.Now().UnixNano())
	traceSubject := natsjetstream.Subject(prefix, natsjetstream.SignalTraces)

	stream, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     streamName,
		Subjects: []string{traceSubject},
		Storage:  jetstream.MemoryStorage,
	})
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}
	t.Cleanup(func() {
		_ = js.DeleteStream(context.Background(), streamName)
	})

	cfg := createDefaultConfig().(*Config)
	cfg.Endpoints = []string{natsjetstream.DefaultEndpoint}
	cfg.SubjectPrefix = prefix
	cfg.Compression = configcompression.TypeZstd

	exp := newExporter(cfg, exporter.Settings{
		ID: component.MustNewID("otlp_nats_jetstream"),
		TelemetrySettings: component.TelemetrySettings{
			Logger: zap.NewNop(),
		},
	})

	if err := exp.start(ctx, nil); err != nil {
		t.Fatalf("start exporter: %v", err)
	}
	t.Cleanup(func() {
		_ = exp.shutdown(context.Background())
	})

	traces := ptrace.NewTraces()
	resourceSpans := traces.ResourceSpans().AppendEmpty()
	scopeSpans := resourceSpans.ScopeSpans().AppendEmpty()
	span := scopeSpans.Spans().AppendEmpty()
	span.SetName("integration-span")

	if err := exp.pushTraces(ctx, traces); err != nil {
		t.Fatalf("push traces: %v", err)
	}

	storedMsg := waitForLastMessage(t, ctx, stream, traceSubject)
	if storedMsg.Subject != traceSubject {
		t.Fatalf("unexpected subject: %s", storedMsg.Subject)
	}
	if storedMsg.Header.Get("Content-Encoding") != "zstd" {
		t.Fatalf("unexpected content encoding: %q", storedMsg.Header.Get("Content-Encoding"))
	}

	payload, err := natsjetstream.DecompressPayload(storedMsg.Data, storedMsg.Header.Get("Content-Encoding"))
	if err != nil {
		t.Fatalf("decompress published payload: %v", err)
	}

	request := ptraceotlp.NewExportRequest()
	if err := request.UnmarshalProto(payload); err != nil {
		t.Fatalf("unmarshal published payload: %v", err)
	}

	gotResourceSpans := request.Traces().ResourceSpans()
	if gotResourceSpans.Len() != 1 {
		t.Fatalf("unexpected resource spans len: %d", gotResourceSpans.Len())
	}
	gotSpans := gotResourceSpans.At(0).ScopeSpans().At(0).Spans()
	if gotSpans.Len() != 1 {
		t.Fatalf("unexpected spans len: %d", gotSpans.Len())
	}
	if gotSpans.At(0).Name() != "integration-span" {
		t.Fatalf("unexpected span name: %s", gotSpans.At(0).Name())
	}
}

func waitForLastMessage(t *testing.T, ctx context.Context, stream jetstream.Stream, subject string) *jetstream.RawStreamMsg {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		msg, err := stream.GetLastMsgForSubject(ctx, subject)
		if err == nil {
			return msg
		}
		if !errors.Is(err, jetstream.ErrMsgNotFound) {
			t.Fatalf("get last message: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}

	t.Fatal("timed out waiting for published message")
	return nil
}
