# OTLP NATS JetStream Receiver

`otlp_nats_jetstream` is an OpenTelemetry Collector receiver that pulls OTLP protobuf payloads from NATS JetStream and forwards them into Collector pipelines.

## What It Does

- Receives traces, metrics, and logs from JetStream pull consumers.
- Routes messages by subject suffix:
  - `.v1.traces`
  - `.v1.metrics`
  - `.v1.logs`
- Acknowledges a message only after the downstream Collector consumer accepts it.
- Uses negative acknowledgement for downstream processing failures.
- Terminates malformed messages instead of redelivering them forever.

## Configuration

Example:

```yaml
receivers:
  otlp_nats_jetstream:
    endpoint:
      - nats://127.0.0.1:4222
    stream:
      # name: otlp
      autodiscover_prefix: otlp.>
    consumer:
      ephemeral: otlp-receiver
    timeout: 5s
```

Supported settings:

- `endpoint`: NATS server endpoint as a string or a list of endpoints. Supported schemes are `nats://`, `tls://`, `ws://`, and `wss://`.
- `tls`: Collector TLS client settings used for the NATS connection.
- `proxy_url`: Proxy configuration for `ws://` and `wss://` endpoints.
- `inbox_prefix`: Custom NATS inbox prefix. Defaults to `_INBOX`.
- `auth`: Optional authentication block. Supported modes are token, username/password, creds file, NKey, JWT alone, or JWT+NKey. Set `auth.jwt` alone to log in with a bearer token, or combine `auth.jwt` with `auth.nkey` to use NATS JWT challenge-response authentication.
- `stream.name`: Explicit stream name to bind to.
- `stream.autodiscover_prefix`: Prefix used to resolve the stream by subject when `stream.name` is not provided. Defaults to `otlp.>`.
- `consumer.durable`: Name of an existing durable pull consumer.
- `consumer.ephemeral`: Name for an ephemeral pull consumer created by the receiver.
- `timeout`: Pull request timeout used by the receive loop.

## Current Notes

- Only pull-based consumers are supported.
- Durable consumers must already exist. The receiver does not create durable consumers.
- Ephemeral consumers are created with `DeliverNewPolicy` and explicit acknowledgements.
- Streams are not created automatically.
- Payload decompression is detected from the message `Content-Encoding` header. A missing header or a value of `none` means the payload is treated as uncompressed; supported compressed encodings match the OTLP/HTTP collector algorithms: `gzip`, `zstd`, `zlib`, `deflate`, `snappy`, `x-snappy-framed`, and `lz4`.
