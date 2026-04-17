# OTLP NATS JetStream Exporter

`otlp_nats_jetstream` is an OpenTelemetry Collector exporter that serializes telemetry as OTLP protobuf and publishes it to NATS JetStream subjects.

## What It Does

- Sends traces, metrics, and logs as OTLP protobuf payloads.
- Publishes to subject names derived from `subject_prefix`:
  - `<subject_prefix>.v1.traces`
  - `<subject_prefix>.v1.metrics`
  - `<subject_prefix>.v1.logs`
- Reuses Collector exporter helper features for timeout, retry, and sending queue behavior.

## Configuration

Example:

```yaml
exporters:
  otlp_nats_jetstream:
    endpoint: nats://127.0.0.1:4222
    subject_prefix: otlp
    timeout: 5s
    compression: gzip
    retry_on_failure:
      enabled: true
    sending_queue:
      enabled: true
    headers:
      x-source: collector
```

Supported settings:

- `endpoint`: NATS server endpoint as a string or a list of endpoints. Supported schemes are `nats://`, `tls://`, `ws://`, and `wss://`.
- `tls`: Collector TLS client settings used for the NATS connection.
- `compression`: Payload compression. Supported values match OTLP/HTTP: `none`, `gzip`, `zstd`, `zlib`, `deflate`, `snappy`, `x-snappy-framed`, and `lz4`. Defaults to `gzip`.
- `compression_params`: Compression tuning compatible with OTLP/HTTP collector settings.
- `proxy_url`: Proxy configuration for `ws://` and `wss://` endpoints.
- `inbox_prefix`: Custom NATS inbox prefix. Defaults to `_INBOX`.
- `auth`: Optional authentication block. Supported mutually exclusive modes are token, username/password, NKey, JWT placeholder, and creds file.
- `subject_prefix`: Prefix used when building JetStream subjects. Defaults to `otlp`.
- `timeout`: Per-attempt exporter timeout from Collector exporter helper.
- `retry_on_failure`: Standard Collector exporter retry settings.
- `sending_queue`: Standard Collector exporter queue settings.
- `headers`: Additional message headers written to the published NATS message.

## Current Notes

- The exporter expects JetStream to be enabled on the target NATS server.
- The first iteration does not implement `expected_stream`.
- JWT auth is not wired yet.
- The exporter does not create streams. Stream provisioning remains an external responsibility.
- When compression is enabled, the exporter writes the standard `Content-Encoding` header on published messages.
