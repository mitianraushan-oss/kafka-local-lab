#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP_SERVER="kafka-1:9092,kafka-2:9092,kafka-3:9092"
CONTAINER="${KAFKA_CONTAINER:-kafka-1}"

docker exec "${CONTAINER}" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --create \
  --if-not-exists \
  --topic orders \
  --partitions 6 \
  --replication-factor 3

docker exec "${CONTAINER}" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --describe \
  --topic orders
