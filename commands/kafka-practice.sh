#!/usr/bin/env bash
set -euo pipefail

CONTAINER="${KAFKA_CONTAINER:-kafka-1}"
BOOTSTRAP_SERVER="kafka-1:9092,kafka-2:9092,kafka-3:9092"
KAFKA_BIN="/opt/kafka/bin"

run_kafka() {
  docker exec "${CONTAINER}" "${KAFKA_BIN}/$1" \
    --bootstrap-server "${BOOTSTRAP_SERVER}" "${@:2}"
}

usage() {
  cat <<'EOF'
Kafka practice commands

  health                    Run broker API checks for each container
  brokers                   List the discovered brokers
  quorum                    Show KRaft quorum status
  topics                    List topics
  create-orders             Create orders with 6 partitions and RF=3
  describe-orders           Describe orders and its replicas
  under-replicated          List under-replicated partitions
  groups                    List consumer groups
  group-lag [GROUP]         Describe group offsets and lag
  produce                   Start an interactive keyed producer for orders
  consume [GROUP]           Consume orders from the beginning
  consume-group GROUP       Consume orders as a member of GROUP
  reset-group GROUP         Preview resetting GROUP to earliest (then confirm)
  perf-produce              Run a bounded producer throughput test
  logs                      Follow logs from all containers
EOF
}

case "${1:-help}" in
  health)
    for broker in kafka-1 kafka-2 kafka-3; do
      printf '%s: ' "${broker}"
      docker exec "${broker}" "${KAFKA_BIN}/kafka-broker-api-versions.sh" \
        --bootstrap-server localhost:9092 >/dev/null
      echo healthy
    done
    ;;
  brokers)
    run_kafka kafka-broker-api-versions.sh
    ;;
  quorum)
    run_kafka kafka-metadata-quorum.sh describe --status
    ;;
  topics)
    run_kafka kafka-topics.sh --list
    ;;
  create-orders)
    run_kafka kafka-topics.sh --create --if-not-exists \
      --topic orders --partitions 6 --replication-factor 3
    run_kafka kafka-topics.sh --describe --topic orders
    ;;
  describe-orders)
    run_kafka kafka-topics.sh --describe --topic orders
    ;;
  under-replicated)
    run_kafka kafka-topics.sh --describe --under-replicated-partitions
    ;;
  groups)
    run_kafka kafka-consumer-groups.sh --list
    ;;
  group-lag)
    run_kafka kafka-consumer-groups.sh --describe --group "${2:-orders-training}"
    ;;
  produce)
    docker exec -it "${CONTAINER}" "${KAFKA_BIN}/kafka-console-producer.sh" \
      --bootstrap-server "${BOOTSTRAP_SERVER}" \
      --topic orders \
      --property "parse.key=true" \
      --property "key.separator=:"
    ;;
  consume)
    run_kafka kafka-console-consumer.sh --topic orders --from-beginning \
      --property print.key=true \
      --property key.separator=: \
      --property print.partition=true \
      --property print.offset=true \
      --property print.timestamp=true
    ;;
  consume-group)
    if [[ -z "${2:-}" ]]; then
      echo "Usage: $0 consume-group GROUP" >&2
      exit 2
    fi
    run_kafka kafka-console-consumer.sh --topic orders \
      --group "$2" --from-beginning \
      --property print.key=true --property key.separator=:
    ;;
  reset-group)
    if [[ -z "${2:-}" ]]; then
      echo "Usage: $0 reset-group GROUP" >&2
      exit 2
    fi
    run_kafka kafka-consumer-groups.sh --group "$2" \
      --reset-offsets --to-earliest --topic orders --dry-run
    printf '\nExecute this reset? [y/N] '
    read -r answer
    if [[ "${answer}" =~ ^[Yy]$ ]]; then
      run_kafka kafka-consumer-groups.sh --group "$2" \
        --reset-offsets --to-earliest --topic orders --execute
    else
      echo "Reset cancelled."
    fi
    ;;
  perf-produce)
    docker exec "${CONTAINER}" "${KAFKA_BIN}/kafka-producer-perf-test.sh" \
      --topic orders \
      --num-records 10000 \
      --record-size 256 \
      --throughput 1000 \
      --producer-props bootstrap.servers="${BOOTSTRAP_SERVER}" acks=all
    ;;
  logs)
    exec docker compose logs -f kafka-1 kafka-2 kafka-3
    ;;
  help|-h|--help)
    usage
    ;;
  *)
    echo "Unknown command: $1" >&2
    usage >&2
    exit 2
    ;;
esac
