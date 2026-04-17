package natsbrokerreceiver

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver"
)

func TestScrapeBuildsVarzAndJszMetrics(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/varz":
			_, _ = w.Write([]byte(`{
				"server_id":"NDX",
				"server_name":"n1",
				"version":"2.11.0",
				"start":"2026-04-17T15:00:00Z",
				"in_msgs":3,
				"out_bytes":9
			}`))
		case "/jsz":
			_, _ = w.Write([]byte(`{
				"server_id":"NDX",
				"config":{"max_memory":1024,"max_storage":2048,"domain":"prod"},
				"streams":2,
				"consumers":3,
				"messages":10,
				"bytes":50,
				"meta_cluster":{"name":"C1","leader":"n1"},
				"account_details":[
					{
						"name":"ACC",
						"id":"A1",
						"memory":10,
						"storage":20,
						"reserved_memory":100,
						"reserved_storage":200,
						"stream_detail":[
							{
								"name":"ORDERS",
								"stream_raft_group":"RG1",
								"cluster":{"leader":"n1"},
								"config":{"max_msgs":-1,"max_bytes":-1},
								"state":{"messages":4,"bytes":1000,"first_seq":1,"last_seq":4,"consumer_count":1,"num_subjects":1},
								"consumer_detail":[
									{
										"name":"dur",
										"config":{"description":"main consumer"},
										"delivered":{"consumer_seq":2,"stream_seq":2},
										"ack_floor":{"consumer_seq":1,"stream_seq":1},
										"num_ack_pending":1,
										"num_redelivered":0,
										"num_waiting":0,
										"num_pending":2,
										"cluster":{"leader":"n1"}
									}
								]
							}
						]
					}
				]
			}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	rcv, err := newReceiver(&Config{
		Endpoints:          []string{server.URL},
		CollectionInterval: 10 * time.Second,
		Timeout:            time.Second,
	}, testReceiverSettings(t), nopMetricsConsumer())
	if err != nil {
		t.Fatalf("create receiver: %v", err)
	}

	client, err := rcv.newHTTPClient()
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	rcv.client = client

	metrics, err := rcv.scrape(context.Background())
	if err != nil {
		t.Fatalf("scrape metrics: %v", err)
	}

	assertGaugeMetric(t, metrics, "gnatsd_varz_in_msgs", 3, map[string]string{
		"server_id": server.URL,
	})
	assertGaugeMetric(t, metrics, "gnatsd_varz_server_name", 1, map[string]string{
		"server_id": server.URL,
		"value":     "n1",
	})
	assertGaugeMetric(t, metrics, "gnatsd_varz_start", float64(time.Date(2026, 4, 17, 15, 0, 0, 0, time.UTC).UnixMilli()), map[string]string{
		"server_id": server.URL,
	})
	assertGaugeMetric(t, metrics, "jetstream_server_total_streams", 2, map[string]string{
		"server_id":      server.URL,
		"server_name":    "n1",
		"cluster":        "C1",
		"domain":         "prod",
		"meta_leader":    "n1",
		"is_meta_leader": "true",
	})
	assertGaugeMetric(t, metrics, "jetstream_stream_total_messages", 4, map[string]string{
		"server_id":         server.URL,
		"account":           "ACC",
		"account_name":      "ACC",
		"account_id":        "A1",
		"stream_name":       "ORDERS",
		"stream_leader":     "n1",
		"is_stream_leader":  "true",
		"stream_raft_group": "RG1",
	})
	assertGaugeMetric(t, metrics, "jetstream_consumer_num_pending", 2, map[string]string{
		"server_id":          server.URL,
		"stream_name":        "ORDERS",
		"consumer_name":      "dur",
		"consumer_desc":      "main consumer",
		"is_consumer_leader": "true",
	})
}

func testReceiverSettings(t *testing.T) receiver.Settings {
	t.Helper()
	return receiver.Settings{
		ID:                component.MustNewID("nats_broker"),
		TelemetrySettings: componenttest.NewNopTelemetrySettings(),
	}
}

func nopMetricsConsumer() consumer.Metrics {
	next, err := consumer.NewMetrics(func(context.Context, pmetric.Metrics) error {
		return nil
	})
	if err != nil {
		panic(err)
	}
	return next
}

func assertGaugeMetric(t *testing.T, metrics pmetric.Metrics, name string, value float64, attrs map[string]string) {
	t.Helper()

	for i := 0; i < metrics.ResourceMetrics().Len(); i++ {
		resourceMetrics := metrics.ResourceMetrics().At(i)
		for j := 0; j < resourceMetrics.ScopeMetrics().Len(); j++ {
			scopeMetrics := resourceMetrics.ScopeMetrics().At(j)
			for k := 0; k < scopeMetrics.Metrics().Len(); k++ {
				metric := scopeMetrics.Metrics().At(k)
				if metric.Name() != name || metric.Type() != pmetric.MetricTypeGauge {
					continue
				}
				points := metric.Gauge().DataPoints()
				for l := 0; l < points.Len(); l++ {
					point := points.At(l)
					if point.DoubleValue() != value {
						continue
					}
					if hasAttributes(point.Attributes(), attrs) {
						return
					}
				}
			}
		}
	}

	t.Fatalf("metric %q with value %v and attrs %v not found", name, value, attrs)
}

func hasAttributes(attributes pcommon.Map, expected map[string]string) bool {
	for key, value := range expected {
		actual, ok := attributes.Get(key)
		if !ok || actual.Str() != value {
			return false
		}
	}
	return true
}
