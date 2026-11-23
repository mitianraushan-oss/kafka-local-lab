#!/usr/bin/env bash
set -euo pipefail

CONTAINER="${KAFKA_CONTAINER:-kafka-1}"
BOOTSTRAP_SERVER="kafka-1:9092,kafka-2:9092,kafka-3:9092"

printf '%s\n' \
  'customer-1001:{"orderId":"ord-1001","customerId":"customer-1001","item":"mechanical-keyboard","quantity":1}' \
  'customer-1002:{"orderId":"ord-1002","customerId":"customer-1002","item":"monitor","quantity":2}' \
  'customer-1001:{"orderId":"ord-1003","customerId":"customer-1001","item":"mouse-pad","quantity":1}' \
  'customer-1003:{"orderId":"ord-1004","customerId":"customer-1003","item":"webcam","quantity":1}' \
  'customer-1002:{"orderId":"ord-1005","customerId":"customer-1002","item":"usb-cable","quantity":3}' \
  | docker exec -i "${CONTAINER}" /opt/kafka/bin/kafka-console-producer.sh \
      --bootstrap-server "${BOOTSTRAP_SERVER}" \
      --topic orders \
      --property "parse.key=true" \
      --property "key.separator=:"

echo "Produced five keyed sample messages to orders."
