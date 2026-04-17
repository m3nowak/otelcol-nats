# based on https://opentelemetry.io/docs/collector/extend/ocb/
FROM docker.io/library/alpine:3.19 AS certs
RUN apk --update add ca-certificates

FROM docker.io/library/golang:1.25 AS build-stage
WORKDIR /build
RUN mkdir -p _build

COPY ./builder-config.local.yaml builder-config.local.yaml
COPY ./receiver ./receiver
COPY ./exporter ./exporter
COPY ./internal ./internal
COPY ./go.mod ./go.mod
COPY ./go.sum ./go.sum

RUN --mount=type=cache,target=/root/.cache/go-build GO111MODULE=on go install go.opentelemetry.io/collector/cmd/builder@v0.150.0
RUN --mount=type=cache,target=/root/.cache/go-build builder --config builder-config.local.yaml

FROM gcr.io/distroless/base:latest

ARG USER_UID=10001
USER ${USER_UID}

COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --chmod=755 --from=build-stage /build/_build/otelcol-nats/otelcol-nats /otelcol

ENTRYPOINT ["/otelcol"]

EXPOSE 4317 4318