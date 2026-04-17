# otelcol-nats

This project is currently experimental and the component surface may still change before a stable `v1.0.0` release.

This repository contains three OpenTelemetry Collector components for NATS:

- `otlp_nats_jetstream` exporter in [exporter/otlpnatsjetstreamexporter](exporter/otlpnatsjetstreamexporter)
- `otlp_nats_jetstream` receiver in [receiver/otlpnatsjetstreamreceiver](receiver/otlpnatsjetstreamreceiver)
- `nats_broker` metrics receiver in [receiver/natsbrokerreceiver](receiver/natsbrokerreceiver)

Current status:

- shared NATS/JetStream config with endpoint and authentication validation
- working exporter that publishes OTLP protobuf payloads to `otlp.v1.*` subjects
- working pull-based receiver with ACK after successful downstream delivery
- builder manifest in [builder-config.local.yaml](builder-config.local.yaml)
- component documentation in [exporter/otlpnatsjetstreamexporter/README.md](exporter/otlpnatsjetstreamexporter/README.md), [receiver/otlpnatsjetstreamreceiver/README.md](receiver/otlpnatsjetstreamreceiver/README.md), and [receiver/natsbrokerreceiver/README.md](receiver/natsbrokerreceiver/README.md)

## Demo

The [demo](demo) directory contains a small runnable example for local verification. It includes a Collector Builder manifest, a sample collector configuration, and a container compose setup for exercising the NATS JetStream transport end to end.

## Development

Local tool requirements are described in [mise.toml](mise.toml). The Collector Builder binary is available as `builder`, not `ocb`.

Example commands:

```bash
go test ./...
mise run build
mise run build-image --tag localhost/otelcol-nats:latest
```

The default development endpoint for the transport components is `nats://127.0.0.1:4222`. The broker metrics receiver defaults to `http://127.0.0.1:8222`.

## Install

Release assets are published on GitHub for `linux/amd64` and `linux/arm64`.

Container images are published to `ghcr.io/m3nowak/otelcol-nats:<version>`.

Example:

```bash
docker run --rm ghcr.io/m3nowak/otelcol-nats:v0.0.1 --version
```

## Run

Example collector configuration:

```yaml
receivers:
	otlp:
		protocols:
			grpc:
			http:

exporters:
	otlp_nats_jetstream:
		endpoint: nats://127.0.0.1:4222
		subject_prefix: otlp

service:
	pipelines:
		traces:
			receivers: [otlp]
			exporters: [otlp_nats_jetstream]
```

Run the released container with a local config file:

```bash
docker run --rm \
	-v "$PWD/otelcol.yaml:/etc/otelcol/config.yaml:ro" \
	ghcr.io/m3nowak/otelcol-nats:v0.0.1 \
	--config /etc/otelcol/config.yaml
```

## Releases

Publishing a GitHub Release triggers the release workflow in GitHub Actions. The release can create the tag in the GitHub UI, and the workflow then builds native `linux/amd64` and `linux/arm64` binaries, attaches both binaries and a `SHA256SUMS` file to that existing release, and publishes the multi-platform container image to `ghcr.io/m3nowak/otelcol-nats`.

## Current limitations

- `jwt` authentication is currently validated at the configuration level only and is not yet wired into the NATS client
- compression matches OTLP/HTTP collector behavior for `gzip`, `zstd`, `zlib`, `deflate`, `snappy`, `x-snappy-framed`, `lz4`, and `none`
