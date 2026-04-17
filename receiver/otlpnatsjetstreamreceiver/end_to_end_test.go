package otlpnatsjetstreamreceiver

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	otlpnatsjetstreamexporter "github.com/m3nowak/otelcol-nats/exporter/otlpnatsjetstreamexporter"
	"github.com/m3nowak/otelcol-nats/internal/natsjetstream"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configcompression"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/receiver"
)

func TestEndToEndTraceFlowAcksMessage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
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

	prefix := fmt.Sprintf("e2e%d", time.Now().UnixNano())
	streamName := fmt.Sprintf("E2E_%d", time.Now().UnixNano())
	traceSubject := natsjetstream.Subject(prefix, natsjetstream.SignalTraces)

	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
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

	receiverFactory := NewFactory()
	receiverCfg := receiverFactory.CreateDefaultConfig().(*Config)
	receiverCfg.Endpoints = []string{natsjetstream.DefaultEndpoint}
	receiverCfg.Stream.Name = streamName
	receiverCfg.Stream.AutodiscoverPrefix = prefix + ".>"
	receiverCfg.Consumer.Ephemeral = fmt.Sprintf("rx_%d", time.Now().UnixNano())
	receiverCfg.Timeout = time.Second

	receivedCh := make(chan ptrace.Traces, 1)
	nextConsumer, err := consumer.NewTraces(func(_ context.Context, traces ptrace.Traces) error {
		cloned := ptrace.NewTraces()
		traces.CopyTo(cloned)
		receivedCh <- cloned
		return nil
	})
	if err != nil {
		t.Fatalf("create test traces consumer: %v", err)
	}

	receiverInstance, err := receiverFactory.CreateTraces(ctx, receiver.Settings{
		ID:                component.MustNewID("otlp_nats_jetstream"),
		TelemetrySettings: componenttest.NewNopTelemetrySettings(),
	}, receiverCfg, nextConsumer)
	if err != nil {
		t.Fatalf("create receiver: %v", err)
	}

	if err := receiverInstance.Start(ctx, nil); err != nil {
		t.Fatalf("start receiver: %v", err)
	}
	t.Cleanup(func() {
		_ = receiverInstance.Shutdown(context.Background())
	})

	exporterFactory := otlpnatsjetstreamexporter.NewFactory()
	exporterCfg := exporterFactory.CreateDefaultConfig().(*otlpnatsjetstreamexporter.Config)
	exporterCfg.Endpoints = []string{natsjetstream.DefaultEndpoint}
	exporterCfg.SubjectPrefix = prefix
	exporterCfg.Compression = configcompression.TypeZstd

	exporterComponent, err := exporterFactory.CreateTraces(ctx, exporter.Settings{
		ID:                component.MustNewID("otlp_nats_jetstream"),
		TelemetrySettings: componenttest.NewNopTelemetrySettings(),
	}, exporterCfg)
	if err != nil {
		t.Fatalf("create exporter: %v", err)
	}

	if err := exporterComponent.Start(ctx, nil); err != nil {
		t.Fatalf("start exporter: %v", err)
	}
	t.Cleanup(func() {
		_ = exporterComponent.Shutdown(context.Background())
	})

	traces := ptrace.NewTraces()
	resourceSpans := traces.ResourceSpans().AppendEmpty()
	scopeSpans := resourceSpans.ScopeSpans().AppendEmpty()
	span := scopeSpans.Spans().AppendEmpty()
	span.SetName("end-to-end-span")

	if err := exporterComponent.ConsumeTraces(ctx, traces); err != nil {
		t.Fatalf("export traces: %v", err)
	}

	select {
	case delivered := <-receivedCh:
		if delivered.ResourceSpans().Len() != 1 {
			t.Fatalf("unexpected resource spans len: %d", delivered.ResourceSpans().Len())
		}
		gotSpans := delivered.ResourceSpans().At(0).ScopeSpans().At(0).Spans()
		if gotSpans.Len() != 1 {
			t.Fatalf("unexpected spans len: %d", gotSpans.Len())
		}
		if gotSpans.At(0).Name() != "end-to-end-span" {
			t.Fatalf("unexpected span name: %s", gotSpans.At(0).Name())
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for receiver output")
	}

	rcv := receiverInstance.(*receiverComponent)
	waitForAck(t, ctx, js, streamName, rcv.consumerName)
}

func TestEndToEndMetricsFlowAcksMessage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
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

	prefix := fmt.Sprintf("e2em%d", time.Now().UnixNano())
	streamName := fmt.Sprintf("E2E_M_%d", time.Now().UnixNano())
	metricsSubject := natsjetstream.Subject(prefix, natsjetstream.SignalMetrics)

	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     streamName,
		Subjects: []string{metricsSubject},
		Storage:  jetstream.MemoryStorage,
	})
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}
	t.Cleanup(func() {
		_ = js.DeleteStream(context.Background(), streamName)
	})

	receiverFactory := NewFactory()
	receiverCfg := receiverFactory.CreateDefaultConfig().(*Config)
	receiverCfg.Endpoints = []string{natsjetstream.DefaultEndpoint}
	receiverCfg.Stream.Name = streamName
	receiverCfg.Stream.AutodiscoverPrefix = prefix + ".>"
	receiverCfg.Consumer.Ephemeral = fmt.Sprintf("rxm_%d", time.Now().UnixNano())
	receiverCfg.Timeout = time.Second

	receivedCh := make(chan pmetric.Metrics, 1)
	nextConsumer, err := consumer.NewMetrics(func(_ context.Context, metrics pmetric.Metrics) error {
		cloned := pmetric.NewMetrics()
		metrics.CopyTo(cloned)
		receivedCh <- cloned
		return nil
	})
	if err != nil {
		t.Fatalf("create test metrics consumer: %v", err)
	}

	receiverInstance, err := receiverFactory.CreateMetrics(ctx, receiver.Settings{
		ID:                component.MustNewID("otlp_nats_jetstream"),
		TelemetrySettings: componenttest.NewNopTelemetrySettings(),
	}, receiverCfg, nextConsumer)
	if err != nil {
		t.Fatalf("create receiver: %v", err)
	}

	if err := receiverInstance.Start(ctx, nil); err != nil {
		t.Fatalf("start receiver: %v", err)
	}
	t.Cleanup(func() {
		_ = receiverInstance.Shutdown(context.Background())
	})

	exporterFactory := otlpnatsjetstreamexporter.NewFactory()
	exporterCfg := exporterFactory.CreateDefaultConfig().(*otlpnatsjetstreamexporter.Config)
	exporterCfg.Endpoints = []string{natsjetstream.DefaultEndpoint}
	exporterCfg.SubjectPrefix = prefix
	exporterCfg.Compression = configcompression.TypeSnappy

	exporterComponent, err := exporterFactory.CreateMetrics(ctx, exporter.Settings{
		ID:                component.MustNewID("otlp_nats_jetstream"),
		TelemetrySettings: componenttest.NewNopTelemetrySettings(),
	}, exporterCfg)
	if err != nil {
		t.Fatalf("create exporter: %v", err)
	}

	if err := exporterComponent.Start(ctx, nil); err != nil {
		t.Fatalf("start exporter: %v", err)
	}
	t.Cleanup(func() {
		_ = exporterComponent.Shutdown(context.Background())
	})

	metrics := pmetric.NewMetrics()
	resourceMetrics := metrics.ResourceMetrics().AppendEmpty()
	scopeMetrics := resourceMetrics.ScopeMetrics().AppendEmpty()
	metric := scopeMetrics.Metrics().AppendEmpty()
	metric.SetName("end-to-end-metric")
	gauge := metric.SetEmptyGauge()
	gauge.DataPoints().AppendEmpty().SetIntValue(42)

	if err := exporterComponent.ConsumeMetrics(ctx, metrics); err != nil {
		t.Fatalf("export metrics: %v", err)
	}

	select {
	case delivered := <-receivedCh:
		if delivered.ResourceMetrics().Len() != 1 {
			t.Fatalf("unexpected resource metrics len: %d", delivered.ResourceMetrics().Len())
		}
		gotMetrics := delivered.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics()
		if gotMetrics.Len() != 1 {
			t.Fatalf("unexpected metrics len: %d", gotMetrics.Len())
		}
		gotMetric := gotMetrics.At(0)
		if gotMetric.Name() != "end-to-end-metric" {
			t.Fatalf("unexpected metric name: %s", gotMetric.Name())
		}
		if gotMetric.Gauge().DataPoints().Len() != 1 {
			t.Fatalf("unexpected data point count: %d", gotMetric.Gauge().DataPoints().Len())
		}
		if gotMetric.Gauge().DataPoints().At(0).IntValue() != 42 {
			t.Fatalf("unexpected data point value: %d", gotMetric.Gauge().DataPoints().At(0).IntValue())
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for receiver output")
	}

	rcv := receiverInstance.(*receiverComponent)
	waitForAck(t, ctx, js, streamName, rcv.consumerName)
}

func TestEndToEndLogsFlowAcksMessage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
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

	prefix := fmt.Sprintf("e2el%d", time.Now().UnixNano())
	streamName := fmt.Sprintf("E2E_L_%d", time.Now().UnixNano())
	logsSubject := natsjetstream.Subject(prefix, natsjetstream.SignalLogs)

	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     streamName,
		Subjects: []string{logsSubject},
		Storage:  jetstream.MemoryStorage,
	})
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}
	t.Cleanup(func() {
		_ = js.DeleteStream(context.Background(), streamName)
	})

	receiverFactory := NewFactory()
	receiverCfg := receiverFactory.CreateDefaultConfig().(*Config)
	receiverCfg.Endpoints = []string{natsjetstream.DefaultEndpoint}
	receiverCfg.Stream.Name = streamName
	receiverCfg.Stream.AutodiscoverPrefix = prefix + ".>"
	receiverCfg.Consumer.Ephemeral = fmt.Sprintf("rxl_%d", time.Now().UnixNano())
	receiverCfg.Timeout = time.Second

	receivedCh := make(chan plog.Logs, 1)
	nextConsumer, err := consumer.NewLogs(func(_ context.Context, logs plog.Logs) error {
		cloned := plog.NewLogs()
		logs.CopyTo(cloned)
		receivedCh <- cloned
		return nil
	})
	if err != nil {
		t.Fatalf("create test logs consumer: %v", err)
	}

	receiverInstance, err := receiverFactory.CreateLogs(ctx, receiver.Settings{
		ID:                component.MustNewID("otlp_nats_jetstream"),
		TelemetrySettings: componenttest.NewNopTelemetrySettings(),
	}, receiverCfg, nextConsumer)
	if err != nil {
		t.Fatalf("create receiver: %v", err)
	}

	if err := receiverInstance.Start(ctx, nil); err != nil {
		t.Fatalf("start receiver: %v", err)
	}
	t.Cleanup(func() {
		_ = receiverInstance.Shutdown(context.Background())
	})

	exporterFactory := otlpnatsjetstreamexporter.NewFactory()
	exporterCfg := exporterFactory.CreateDefaultConfig().(*otlpnatsjetstreamexporter.Config)
	exporterCfg.Endpoints = []string{natsjetstream.DefaultEndpoint}
	exporterCfg.SubjectPrefix = prefix

	exporterComponent, err := exporterFactory.CreateLogs(ctx, exporter.Settings{
		ID:                component.MustNewID("otlp_nats_jetstream"),
		TelemetrySettings: componenttest.NewNopTelemetrySettings(),
	}, exporterCfg)
	if err != nil {
		t.Fatalf("create exporter: %v", err)
	}

	if err := exporterComponent.Start(ctx, nil); err != nil {
		t.Fatalf("start exporter: %v", err)
	}
	t.Cleanup(func() {
		_ = exporterComponent.Shutdown(context.Background())
	})

	logs := plog.NewLogs()
	resourceLogs := logs.ResourceLogs().AppendEmpty()
	scopeLogs := resourceLogs.ScopeLogs().AppendEmpty()
	logRecord := scopeLogs.LogRecords().AppendEmpty()
	logRecord.Body().SetStr("end-to-end-log")

	if err := exporterComponent.ConsumeLogs(ctx, logs); err != nil {
		t.Fatalf("export logs: %v", err)
	}

	select {
	case delivered := <-receivedCh:
		if delivered.ResourceLogs().Len() != 1 {
			t.Fatalf("unexpected resource logs len: %d", delivered.ResourceLogs().Len())
		}
		gotLogs := delivered.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords()
		if gotLogs.Len() != 1 {
			t.Fatalf("unexpected log records len: %d", gotLogs.Len())
		}
		if gotLogs.At(0).Body().AsString() != "end-to-end-log" {
			t.Fatalf("unexpected log body: %s", gotLogs.At(0).Body().AsString())
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for receiver output")
	}

	rcv := receiverInstance.(*receiverComponent)
	waitForAck(t, ctx, js, streamName, rcv.consumerName)
}

func waitForAck(t *testing.T, ctx context.Context, js jetstream.JetStream, streamName, consumerName string) {
	t.Helper()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		consumerInstance, err := js.Consumer(ctx, streamName, consumerName)
		if err != nil {
			t.Fatalf("load consumer info: %v", err)
		}
		info, err := consumerInstance.Info(ctx)
		if err != nil {
			t.Fatalf("read consumer info: %v", err)
		}
		if info.NumAckPending == 0 && info.AckFloor.Consumer > 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}

	t.Fatalf("timed out waiting for consumer %s to ack messages", consumerName)
}
