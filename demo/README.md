# Demo
This demo requires [mise](https://mise.jdx.dev/getting-started.html) to be installed and configured

1. `mise trust . -r`
2. `mise install`
3. `mise r serve ::: build`
4. In separate terminal: `mise r gen-telemetry`
5. Navigate in browser to http://127.0.0.1:3000 and browse generated telemetry

The demo shows two data paths:

- traces and logs flow through `otlp_nats_jetstream`
- NATS broker metrics are scraped by `nats_broker` and sent directly to LGTM over OTLP HTTP

```mermaid
flowchart TD
    R1[OLTP-HTTP Receiver :14318]
    R2[Nats Broker Receiver]
    R3[OLTP Nats Jetstream Receiver]
    
    E1[OLTP Nats Jetstream Exporter]
    E2[OLTP-HTTP Exporter]
    
    NBM[Nats Broker Monitoring :8222]
    NB[Nats Broker :4222]
    LGTM[Grafana LGTM Stack :4318]

    R1 --M+T+L--> E1
    E1 -- pub otlp.v1.*--> NB
    NB --consumer otlp-consumer--> R3
    NBM --HTTP--> R2
    R2 --Metrics--> E2
    R3 --M+T+L--> E2
    E2 --M+T+L--> LGTM

    A[M+T+L = Metrics, Logs and Traces]@{ shape: card }
```

Note: Grafana in LGTM seems to display metrics with 1m resolution, despite Nats Broker Receiver sending them every 10s.