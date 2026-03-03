# platformatic-kafka-minimum-demo

Minimum reproducible NestJS repository for testing `@platformatic/kafka` consumption and forwarding throughput.

It keeps the same basic shape as a service app (Nest module, controller, service, Kafka consumer service):

1. Kafka inbound consumer subscribes to `platformic.minimum.demo`.
2. The message is handled by a dedicated consumer class and service.
3. The service forwards the payload to `platformic.uplink.demo`.


## Prerequisites

- Node.js 22.19+
- Docker

## Setup

```bash
docker compose up -d
npm install
```

## Run the Nest app

```bash
npm run start:dev
```

Key endpoints:

- `GET /api/v1/health`
- `GET /api/v1/demo/hi`
- `POST /api/v1/demo/send`

Example publish via HTTP:

```bash
curl -X POST "http://localhost:3000/api/v1/demo/send" \
  -H "content-type: application/json" \
  -d '{"key":"demo-1","value":"hello"}'
```

## Benchmark helpers

In a second terminal, produce source-topic load:

```bash
COUNT=100000 BATCH_SIZE=1000 npm run benchmark:produce
```

In a third terminal, consume uplink topic and report throughput:

```bash
EXPECTED_COUNT=100000 npm run benchmark:uplink
```

## Environment variables

- `PORT` (default: `3000`)
- `KAFKA_BROKERS` (default: `localhost:19092`)
- `KAFKA_GROUP_ID` (default: `platformic-minimum-demo-group`)
- `COUNT` (default: `100000`)
- `BATCH_SIZE` (default: `1000`)
- `PAYLOAD_SIZE` (default: `128`)
- `GROUP_ID` (for benchmark uplink listener group id)
- `EXPECTED_COUNT` (default: `0`, disabled)
