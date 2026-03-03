# Kafka Benchmark Harness

This project runs two minimal Nest services that mirror the Kafka integration from the
`{our service}` branches and a local Apache Kafka broker (KRaft mode) for benchmarking.

## Quick Start

1. Build and start the stack:

```bash
docker compose up -d --build
```

2. Run the existing stress test against the broker:

```bash
node ./scripts/kafka-stress.e2e.js
```

## Container Stats + Graphs

Collect stats (defaults to both benchmark services). Omit duration to run until Ctrl+C:

```bash
./scripts/collect-stats.sh stats.csv 1
```

Timed capture (seconds):

```bash
./scripts/collect-stats.sh stats.csv 1 60 benchmark-platformatic benchmark-confluent
```

Render graphs:

```bash
node ./scripts/render-stats.js stats.csv stats.html
```

## External Broker

To point the services at an external Kafka broker, edit the broker list in:

- `kafka-benchmark/services/platformatic-nest/config/config.json`
- `kafka-benchmark/services/confluent-nest/config/config.json`

Then rebuild the services:

```bash
docker compose build
```
