# otelcol-nats

This repository contains two OpenTelemetry Collector components for OTLP over NATS JetStream transport:

- `otlp_nats_jetstream` exporter in [exporter/otlpnatsjetstreamexporter](exporter/otlpnatsjetstreamexporter)
- `otlp_nats_jetstream` receiver in [receiver/otlpnatsjetstreamreceiver](receiver/otlpnatsjetstreamreceiver)

Current status:

- shared NATS/JetStream config with endpoint and authentication validation
- working exporter that publishes OTLP protobuf payloads to `otlp.v1.*` subjects
- working pull-based receiver with ACK after successful downstream delivery
- local builder manifest in [builder-config.yaml](builder-config.yaml)
- component documentation in [exporter/otlpnatsjetstreamexporter/README.md](exporter/otlpnatsjetstreamexporter/README.md) and [receiver/otlpnatsjetstreamreceiver/README.md](receiver/otlpnatsjetstreamreceiver/README.md)

## Demo

The [demo](demo) directory contains a small runnable example for local verification. It includes a Collector Builder manifest, a sample collector configuration, and a container compose setup for exercising the NATS JetStream transport end to end.

## Development

Local tool requirements are described in [mise.toml](mise.toml). The Collector Builder binary is available as `builder`, not `ocb`.

Example commands:

```bash
go test ./...
builder --config=builder-config.yaml
```

The default development endpoint for the components is `nats://127.0.0.1:4222`.
