# NATS Broker Receiver

`nats_broker` is an OpenTelemetry Collector metrics receiver that scrapes NATS monitoring endpoints and converts broker metrics into OTLP metrics.

## What It Does

- Scrapes core broker statistics from `/varz`.
- Scrapes JetStream statistics from `/jsz`.
- Keeps metric and label names close to `prometheus-nats-exporter`.
- Supports one or many monitoring endpoints.

## Configuration

Example:

```yaml
receivers:
  nats_broker:
    endpoint:
      - http://127.0.0.1:8222
    collection_interval: 10s
    timeout: 5s
```

Supported settings:

- `endpoint`: NATS monitoring endpoint as a string or a list of endpoints. Supported schemes are `http://` and `https://`.
- `tls`: Collector TLS client settings for HTTPS monitoring endpoints.
- `auth.username`: Optional HTTP basic auth username.
- `auth.password`: Optional HTTP basic auth password.
- `collection_interval`: Scrape interval.
- `timeout`: Per-request timeout.

## Current Notes

- The receiver currently scrapes `/varz` and `/jsz?consumers=true&config=true&raft=true`.
- JetStream metrics are skipped for endpoints where `/jsz` is unavailable.
- Metric names intentionally stay close to `prometheus-nats-exporter` for easier dashboard reuse.
