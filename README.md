# Local Apache Kafka Administration Lab

This lab runs a three-node Apache Kafka 3.9.1 cluster in KRaft mode. Every node is both a broker and a KRaft controller. There is no ZooKeeper. Kafka UI is available at [http://localhost:8080](http://localhost:8080).

> This configuration is for local education only. It uses `PLAINTEXT`, permits DYNAMIC_CONFIG in Kafka UI, and does not provide production security, backup, or disaster-recovery controls.

## Architecture

| Service | Node ID | Internal address | Host address | Persistent volume |
| --- | ---: | --- | --- | --- |
| `kafka-1` | 1 | `kafka-1:9092` | `localhost:19092` | `kafka-lab-kafka-1-data` |
| `kafka-2` | 2 | `kafka-2:9092` | `localhost:29092` | `kafka-lab-kafka-2-data` |
| `kafka-3` | 3 | `kafka-3:9092` | `localhost:39092` | `kafka-lab-kafka-3-data` |
| `kafka-ui` | - | `kafka-1:9092`... | [localhost:8080](http://localhost:8080) | none |

The `INTERNAL` listener is used for Docker-to-Docker traffic. The `EXTERNAL` listener is advertised as `localhost` for host applications. The controller listener is private to the Compose network. All three nodes are controller voters, so two controllers must be available to maintain quorum.

The default learning settings are:

- default and offsets-topic replication factor: 3
- minimum in-sync replicas: 2
- transaction-state replication factor: 3 and minimum ISR: 2
- automatic topic creation: disabled
- seven-day default retention
- bounded JVM heaps and container memory limits

## Prerequisites

- Docker Desktop (Linux containers mode)
- Docker Compose v2 (`docker compose`)
- At least 4 GB of available RAM; 6 GB is more comfortable
- Free host ports 8080, 19092, 29092, and 39092

Run the commands below from this directory:

```text
apps\python_program\kafka-local-lab
```

## Start and verify the lab

### Start the cluster

Linux/macOS or PowerShell:

```sh
docker compose up -d
docker compose ps
```

Kafka UI starts only after all three brokers are healthy. Open [http://localhost:8080](http://localhost:8080).

### Check container and broker health

```sh
docker compose ps
docker compose logs --tail=100 kafka-1 kafka-2 kafka-3
docker exec kafka-1 /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092
docker inspect -f "{{.State.Health.Status}}" kafka-1
docker inspect -f "{{.State.Health.Status}}" kafka-2
docker inspect -f "{{.State.Health.Status}}" kafka-3
```

PowerShell quoting does not differ for these commands. A broker is ready when its inspect result is `healthy`.

### Inspect the KRaft quorum

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server kafka-1:9092 describe --status
docker exec kafka-1 /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server kafka-1:9092 describe --replication
```

### List brokers

KRaft has no ZooKeeper-based broker listing command. The API-version tool discovers and prints the running broker endpoints:

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092
```

The output should show broker IDs `1`, `2`, and `3`. Kafka UI also displays them under **Brokers**.

## Core command-line lab

The commands below deliberately run Kafka CLIs inside `kafka-1`, so no host Kafka installation is needed. `-it` is used only for interactive commands. Omit `-t` in CI or a non-TTY shell.

### Create `orders`

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 --create --topic orders --partitions 6 --replication-factor 3
```

Or use the helper:

```sh
bash scripts/create-topics.sh
```

In PowerShell without Git Bash:

```powershell
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 --create --topic orders --partitions 6 --replication-factor 3
```

### List topics and describe `orders`

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 --list
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 --describe --topic orders
```

`Replicas` lists all copies; `Isr` lists copies currently caught up. Every partition should have three replicas and a three-member ISR.

### Increase partitions from 6 to 9

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 --alter --topic orders --partitions 9
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 --describe --topic orders
```

Partition count can only increase. Existing keyed records cannot be redistributed, so changing it may change the partition selected for future occurrences of a key.

### Set retention to one hour

One hour is 3,600,000 milliseconds:

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 --entity-type topics --entity-name orders --alter --add-config retention.ms=3600000
```

### View topic configuration

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 --entity-type topics --entity-name orders --describe
```

`retention.ms=3600000` appears for the topic. To revert to the broker default:

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 --entity-type topics --entity-name orders --alter --delete-config retention.ms
```

### Produce messages with keys

Interactive keyed producer:

```sh
docker exec -it kafka-1 /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 --topic orders --property "parse.key=true" --property "key.separator=:"
```

Type one record per line:

```text
customer-1001:{"orderId":"ord-1001","total":49.99}
customer-1001:{"orderId":"ord-1002","total":12.50}
customer-1002:{"orderId":"ord-1003","total":89.00}
```

Press `Ctrl+C` to stop. The same key is sent to the same partition as long as the partition count is unchanged.

Non-interactive sample data:

```sh
bash scripts/produce-sample-data.sh
```

PowerShell equivalent:

```powershell
@(
  'customer-1001:{"orderId":"ord-1001","customerId":"customer-1001","total":49.99}',
  'customer-1002:{"orderId":"ord-1002","customerId":"customer-1002","total":89.00}',
  'customer-1001:{"orderId":"ord-1003","customerId":"customer-1001","total":12.50}'
) | docker exec -i kafka-1 /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 --topic orders --property parse.key=true --property key.separator=:
```

### Consume from the beginning

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 --topic orders --from-beginning
```

### Print key, partition, offset, and timestamp

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 --topic orders --from-beginning --property print.key=true --property key.separator=: --property print.partition=true --property print.offset=true --property print.timestamp=true
```

Kafka 3.x output has `CreateTime`, partition, offset, key, and value. Press `Ctrl+C` after observing it.

### Multiple consumers in one consumer group

Open as many terminals as desired. Run this in terminal 1:

```sh
docker exec -it kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 --topic orders --group orders-training --from-beginning
```

Run this at the same time in terminal 2:

```sh
docker exec -it kafka-2 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 --topic orders --group orders-training --from-beginning
```

Run it in terminal 3 for a third group member:

```sh
docker exec -it kafka-3 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 --topic orders --group orders-training --from-beginning
```

### Describe group offsets and lag

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 --describe --group orders-training
```

`CURRENT-OFFSET` is the committed position. `LOG-END-OFFSET` is the newest position available. `LAG` is the difference.

### Reset group offsets safely

1. Stop all consumers in the group. Kafka refuses a reset while active members are processing assignments.
2. Preview first:

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 --group orders-training --topic orders --reset-offsets --to-earliest --dry-run
```

3. Execute only after reviewing the planned offsets:

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 --group orders-training --topic orders --reset-offsets --to-earliest --execute
```

4. Verify:

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 --describe --group orders-training
```

Other useful reset targets are `--to-latest`, `--to-offset 10`, and `--to-datetime 2026-01-01T00:00:00.000`.

### List under-replicated partitions and assignments

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 --describe --under-replicated-partitions
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 --describe --topic orders
docker exec kafka-1 /opt/kafka/bin/kafka-log-dirs.sh --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 --describe
```

The first command should print nothing in a healthy cluster. The topic description shows leaders and `Replicas`/`Isr`; `kafka-log-dirs` shows physical replica sizes by broker.

### Reassign partitions

Reassignment is an online operation but consumes disk, network, and broker I/O. Create the move request inside `kafka-1`.

Linux/macOS:

```sh
docker exec kafka-1 bash -c 'printf "%s\n" "{\"version\":1,\"topics\":[{\"topic\":\"orders\"}]}" > /tmp/topics-to-move.json'
docker exec kafka-1 /opt/kafka/bin/kafka-reassign-partitions.sh --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 --generate --topics-to-move-json-file /tmp/topics-to-move.json --broker-list "1,2,3"
```

PowerShell:

```powershell
'{"version":1,"topics":[{"topic":"orders"}]}' | docker exec -i kafka-1 tee /tmp/topics-to-move.json
docker exec kafka-1 /opt/kafka/bin/kafka-reassign-partitions.sh --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 --generate --topics-to-move-json-file /tmp/topics-to-move.json --broker-list 1,2,3
```

Save the JSON printed after `Proposed partition reassignment configuration` to `/tmp/reassignment.json`, or use this deterministic six-partition example. It rotates each partition's leader preference and preserves replication factor 3:

Linux/macOS:

```sh
docker exec kafka-1 bash -c 'cat > /tmp/reassignment.json <<EOF
{"version":1,"partitions":[{"topic":"orders","partition":0,"replicas":[1,2,3]},{"topic":"orders","partition":1,"replicas":[2,3,1]},{"topic":"orders","partition":2,"replicas":[3,1,2]},{"topic":"orders","partition":3,"replicas":[1,3,2]},{"topic":"orders","partition":4,"replicas":[2,1,3]},{"topic":"orders","partition":5,"replicas":[3,2,1]}]}
EOF
'
```

PowerShell:

```powershell
@'
{"version":1,"partitions":[{"topic":"orders","partition":0,"replicas":[1,2,3]},{"topic":"orders","partition":1,"replicas":[2,3,1]},{"topic":"orders","partition":2,"replicas":[3,1,2]},{"topic":"orders","partition":3,"replicas":[1,3,2]},{"topic":"orders","partition":4,"replicas":[2,1,3]},{"topic":"orders","partition":5,"replicas":[3,2,1]}]}
'@ | docker exec -i kafka-1 tee /tmp/reassignment.json
```

Execute and monitor:

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-reassign-partitions.sh --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 --execute --reassignment-json-file /tmp/reassignment.json
docker exec kafka-1 /opt/kafka/bin/kafka-reassign-partitions.sh --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 --verify --reassignment-json-file /tmp/reassignment.json
```

If `orders` has already been expanded beyond six partitions, generate the complete plan instead of using the six-partition example.

### Change the preferred partition leader

For all partitions, ask Kafka to elect the first broker in each partition's `Replicas` list:

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-leader-election.sh --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 --election-type PREFERRED --all-topic-partitions
```

Verify before and after with:

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 --describe --topic orders
```

Preferred leader election is useful after a failed leader returns. Kafka can also perform unclean leader election only when explicitly configured, but this lab does not rely on it because it risks data loss.

### Delete a topic

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 --delete --topic orders
```

Deletion removes records and configurations after the controller processes it. Verify with `--list`.

## Broker failure and recovery

### Stop and restart one broker

```sh
docker stop kafka-2
docker compose ps
```

Use `kafka-1` to watch the surviving cluster:

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092,kafka-3:9092 --describe --topic orders
docker exec kafka-1 /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server kafka-1:9092 describe --status
```

Partitions led by broker 2 elect a surviving replica as leader. Broker 2 disappears from each affected ISR. Because the other two replicas remain, `min.insync.replicas=2` producers can still write with `acks=all` and healthy cluster majority remains.

Restart:

```sh
docker start kafka-2
docker inspect -f "{{.State.Health.Status}}" kafka-2
```

Watch recovery:

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 --describe --under-replicated-partitions
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 --describe --topic orders
```

The URP command eventually returns no rows, and the ISR of every partition again contains IDs `1,2,3`. That proves replicas caught up. Preferred leaders do not necessarily move back automatically; use the leader-election command if desired.

## Stop or reset

### Stop without deleting data

```sh
docker compose stop
```

Restart the same data:

```sh
docker compose start
```

Do not use `down --volumes` for this operation. `docker compose down` preserves named volumes but recreates containers; it is also non-destructive to data.

### Complete reset, including volumes

```sh
docker compose down --volumes --remove-orphans
```

Helper:

```sh
bash scripts/reset-lab.sh
```

Then start fresh:

```sh
docker compose up -d
```

These commands delete all messages, topics, group offsets, and KRaft metadata in the lab's three named volumes.

## Structured learning path

Run the exercises with `orders` created and, unless stated, with six partitions and replication factor 3. Create it at any point with:

```sh
bash scripts/create-topics.sh
```

### Level 1: Topics, producers and consumers

**Objective:** Understand a topic as an ordered log and verify basic production and consumption.

**Commands:**

```sh
bash scripts/create-topics.sh
bash scripts/produce-sample-data.sh
docker exec kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka-1:9092 --topic orders --from-beginning --timeout-ms 10000
```

**Expected result:** Five sample records are visible. The create helper reports six partitions.

**Explanation:** Producers append records. Consumers read records without deleting them, using an offset as their position.

**Verification command:**

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --describe --topic orders
```

**Cleanup command:**

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --delete --topic orders
```

**Troubleshooting questions:**

1. Does `orders` exist, and do the broker logs show a topic-creation error?
2. Are all three containers healthy before the producer starts?

### Level 2: Partitions, keys and message ordering

**Objective:** Prove that records with the same key go to the same partition and remain ordered there.

**Commands:**

```sh
docker exec -it kafka-1 /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka-1:9092 --topic orders --property parse.key=true --property key.separator=:
```

Enter `same-key:first`, then `same-key:second`, then `other-key:third`.

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka-1:9092 --topic orders --from-beginning --property print.key=true --property print.partition=true --property print.offset=true
```

**Expected result:** `same-key:first` and `same-key:second` have the same partition and increasing offsets.

**Explanation:** Kafka's partitioner hashes a key. Ordering is guaranteed per partition, not globally across the topic.

**Verification command:**

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka-1:9092 --topic orders --from-beginning --property print.key=true --property print.partition=true --property print.offset=true --timeout-ms 10000
```

**Cleanup command:** No topic cleanup is needed. A clean result requires deleting and recreating `orders`.

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --delete --topic orders
```

**Troubleshooting questions:**

1. Did the producer enable `parse.key=true`, or are colons being stored only as values?
2. Did the topic's partition count change while keys were being compared?

### Level 3: Consumer groups and partition assignment

**Objective:** Observe load sharing and rebalancing among consumers in one group.

**Commands:**

Open two terminals and run the same command with one change to the container:

```sh
docker exec -it kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka-1:9092 --topic orders --group level3-group --from-beginning
```

```sh
docker exec -it kafka-2 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka-1:9092 --topic orders --group level3-group --from-beginning
```

Keep both alive, then run:

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:9092 --describe --group level3-group
```

**Expected result:** Partitions are divided between the two consumers; no partition has two members at the same time.

**Explanation:** Consumer-group coordination creates one exclusive owner per partition. Adding and removing members triggers a rebalance.

**Verification command:**

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:9092 --describe --group level3-group --members --verbose
```

**Cleanup command:** Stop both consumers, then:

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:9092 --delete --group level3-group
```

**Troubleshooting questions:**

1. Do the consumers use exactly the same group ID?
2. Did both members start before the assignment was inspected?

### Level 4: Replication, leaders, followers and ISR

**Objective:** Relate replica assignment, preferred leader, and caught-up followers.

**Commands:**

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --describe --topic orders
docker exec kafka-1 /opt/kafka/bin/kafka-log-dirs.sh --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 --describe --broker-list 1,2,3
```

**Expected result:** Every `orders` partition has three replicas, one leader, and all three replicas in `Isr`.

**Explanation:** Replicas are copies. The leader serves writes; followers fetch from it. ISR identifies replicas sufficiently caught up for strongly acknowledged writes.

**Verification command:**

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --describe --under-replicated-partitions
```

**Cleanup command:** No cleanup is required.

**Troubleshooting questions:**

1. Is a broker unhealthy or missing from the KRaft quorum?
2. Is a replica far behind because of disk, network, or broker I/O pressure?

### Level 5: Consumer lag and offset management

**Objective:** Measure lag, then replay records safely.

**Commands:**

```sh
bash scripts/produce-sample-data.sh
docker exec kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka-1:9092 --topic orders --group lag-lab --timeout-ms 3000 --from-beginning >/dev/null
docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:9092 --describe --group lag-lab
```

PowerShell substitutes `$null` for `/dev/null` in the middle command. Produce more while consumers are stopped and repeat the describe command.

**Expected result:** Lag increases after new production and falls after a consumer processes the records.

**Explanation:** Kafka stores consumer progress per group and topic partition. Offset operations must not race with active group members.

**Verification command:**

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:9092 --group lag-lab --topic orders --reset-offsets --to-earliest --dry-run
```

**Cleanup command:**

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:9092 --delete --group lag-lab
```

**Troubleshooting questions:**

1. Is lag growing because production rate exceeds processing rate?
2. Is the consumer repeatedly failing before committing its processed offset?

### Level 6: Broker failure and recovery

**Objective:** Observe automatic partition leadership changes without losing quorum.

**Commands:**

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --describe --topic orders
docker stop kafka-2
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092,kafka-3:9092 --describe --topic orders
docker start kafka-2
```

**Expected result:** Partitions led by broker 2 elect broker 1 or 3. They have only two ISRs while broker 2 is down, then all recover.

**Explanation:** KRaft elects leaders from in-sync replicas. Stopping one of three combined nodes leaves a two-node quorum, so availability remains.

**Verification command:**

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 --describe --under-replicated-partitions
```

**Cleanup command:**

```sh
docker start kafka-2
docker exec kafka-1 /opt/kafka/bin/kafka-leader-election.sh --bootstrap-server kafka-1:9092 --election-type PREFERRED --all-topic-partitions
```

**Troubleshooting questions:**

1. Were two brokers accidentally stopped, leaving no KRaft majority?
2. Has the restarted broker remained unavailable due to a listener or disk error?

### Level 7: Topic configuration and retention

**Objective:** Override retention per topic and understand asynchronous segment deletion.

**Commands:**

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka-1:9092 --entity-type topics --entity-name orders --alter --add-config retention.ms=3600000
docker exec kafka-1 /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka-1:9092 --entity-type topics --entity-name orders --describe
```

**Expected result:** The configuration output includes `retention.ms=3600000`.

**Explanation:** Retention is applied by segment cleanup. It is not an immediate record TTL that deletes a record exactly at its one-hour boundary.

**Verification command:**

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-log-dirs.sh --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 --describe --topic-list orders
```

**Cleanup command:**

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka-1:9092 --entity-type topics --entity-name orders --alter --delete-config retention.ms
```

**Troubleshooting questions:**

1. Is the segment newer than `segment.ms` and therefore not yet eligible for retention?
2. Is an open active segment preventing old records from being reclaimed?

### Level 8: Partition reassignment and balancing

**Objective:** Move replicas deliberately and verify convergence.

**Commands:** Follow the reassignment procedure in the earlier **Reassign partitions** section, beginning with:

```sh
docker exec kafka-1 bash -c 'printf "%s\n" "{\"version\":1,\"topics\":[{\"topic\":\"orders\"}]}" > /tmp/topics-to-move.json'
docker exec kafka-1 /opt/kafka/bin/kafka-reassign-partitions.sh --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 --generate --topics-to-move-json-file /tmp/topics-to-move.json --broker-list "1,2,3"
```

**Expected result:** A generated plan, followed by a successful execute and a verify message showing completion.

**Explanation:** Reassignment copies data to new broker replicas before removing old ones. This temporarily raises network and disk utilization.

**Verification command:**

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-reassign-partitions.sh --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 --list
```

**Cleanup command:** There is no partial cleanup for a complete reassignment. To cancel, run the documented `--cancel` command with its reassignment JSON only if an active reassignment is genuinely in progress:

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-reassign-partitions.sh --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 --cancel
```

**Troubleshooting questions:**

1. Is another reassignment already active for one of the requested partitions?
2. Are broker disks or network links saturating during replica movement?

### Level 9: Performance testing

**Objective:** Establish a local baseline for producer and consumer throughput.

**Commands:**

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-producer-perf-test.sh --topic orders --num-records 50000 --record-size 512 --throughput 2000 --producer-props bootstrap.servers=kafka-1:9092,kafka-2:9092,kafka-3:9092 acks=all linger.ms=5 batch.size=16384
docker exec kafka-1 /opt/kafka/bin/kafka-consumer-perf-test.sh --bootstrap-server kafka-1:9092 --topic orders --messages 50000 --timeout 120000 --group perf-lab
```

**Expected result:** The tools report records/sec, MB/sec, and latency summaries. Values are relative to this laptop and Docker, not production capacity estimates.

**Explanation:** Batching, compression, acknowledgements, record size, disk, CPU, and network all affect throughput. Run repeated tests and change one variable at a time.

**Verification command:**

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:9092 --describe --group perf-lab
```

**Cleanup command:**

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:9092 --delete --group perf-lab
```

**Troubleshooting questions:**

1. Did the requested throughput make this a rate-limited test rather than a capacity test?
2. Is the bottleneck CPU, Docker filesystem I/O, producer batching, or broker disk?

### Level 10: Troubleshooting scenarios

**Objective:** Use evidence from metadata, logs, replica state, and offsets to diagnose an incident.

**Commands:**

```sh
docker compose ps
docker compose logs --tail=200 kafka-1 kafka-2 kafka-3
docker exec kafka-1 /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server kafka-1:9092 describe --status
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --describe --under-replicated-partitions
docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:9092 --describe --all-groups
```

**Expected result:** You can identify the broken layer: container, network/listener, quorum, replica, topic, producer, or consumer group.

**Explanation:** Start broad and narrow down. Establish process health, controller quorum, broker availability, replica state, and consumer offsets before changing configuration.

**Verification command:**

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092
```

**Cleanup command:**

```sh
docker compose restart kafka-1 kafka-2 kafka-3
```

**Troubleshooting questions:**

1. Did the symptom begin after a broker, network, topic configuration, or application deployment changed?
2. Does the issue affect all partitions and clients or only one partition, broker, or consumer group?

## SRE incident scenarios

### 1. One broker becomes unavailable

**Symptoms:** One container is stopped or unhealthy; some partitions have a smaller ISR; clients reconnecting to that broker receive connection errors.

**Investigation commands:**

```sh
docker compose ps
docker logs --tail=200 kafka-2
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --describe --under-replicated-partitions
docker exec kafka-1 /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server kafka-1:9092 describe --status
```

**Likely cause:** A process crash, memory pressure, volume failure, Docker resource issue, or deliberate stop.

**Remediation:** Check logs and Docker resources, then run `docker start kafka-2` or `docker compose up -d kafka-2`. Do not restart a healthy quorum member at the same time as another member.

**Post-fix verification:** Wait for health:

```sh
docker inspect -f "{{.State.Health.Status}}" kafka-2
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 --describe --under-replicated-partitions
```

No under-replicated rows should remain.

### 2. A partition becomes under-replicated

**Symptoms:** `Isr` contains fewer brokers than `Replicas`; `kafka-topics --describe` flags the partition; producer latency can increase.

**Investigation commands:**

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --describe --under-replicated-partitions
docker exec kafka-1 /opt/kafka/bin/kafka-log-dirs.sh --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 --describe
docker compose stats --no-stream kafka-1 kafka-2 kafka-3
```

**Likely cause:** A follower is down, restarting, slow on disk/network, or overloaded enough to exceed the replica lag setting.

**Remediation:** Restore broker health and resources. Do not reassign a partition until transient catch-up has been allowed and replica infrastructure is healthy.

**Post-fix verification:**

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --describe --topic orders
```

`Isr` should match `Replicas`.

### 3. Consumer lag continuously increases

**Symptoms:** `LAG` increases on every describe interval; end offsets move faster than committed offsets.

**Investigation commands:**

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:9092 --describe --group orders-training
docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:9092 --describe --group orders-training --members --verbose
```

**Likely cause:** Consumers are too slow, failing, processing larger batches than expected, or fewer active consumers exist than available partitions.

**Remediation:** Fix application errors, scale consumers up to the partition count, improve batch/transaction handling, or reduce ingress. Do not reset offsets as the first response; that can reintroduce already processed work.

**Post-fix verification:** Record `LAG` twice and confirm it trends toward zero:

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:9092 --describe --group orders-training
```

### 4. A consumer group has more consumers than partitions

**Symptoms:** Group describe output shows active members with no assigned partitions; some clients appear idle.

**Investigation commands:**

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:9092 --describe --group orders-training --members --verbose
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --describe --topic orders
```

**Likely cause:** Kafka assigns at most one group member per partition.

**Remediation:** Scale consumers down to the partition count, or deliberately increase partitions only when key distribution, retention, and broker capacity are considered. Idle group members are harmless but waste resources.

**Post-fix verification:** Each active member should own at least one partition, except an intentional standby-like extra that remains idle:

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:9092 --describe --group orders-training --members --verbose
```

### 5. Messages with the same key must remain ordered

**Symptoms:** An application observes records for one business entity in a different order than expected.

**Investigation commands:**

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka-1:9092 --topic orders --from-beginning --property print.key=true --property print.partition=true --property print.offset=true --timeout-ms 10000
docker exec kafka-1 /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka-1:9092 --entity-type topics --entity-name orders --describe
```

**Likely cause:** Missing keys, changing partition counts, custom partitioning, competing producers, or retries without idempotence can defeat ordering assumptions.

**Remediation:** Use a stable non-null key, enable producer idempotence, avoid changing partition count for ordering-sensitive migrations, and preserve the producer's partitioner behavior. Consume and process concurrently per partition rather than globally.

**Post-fix verification:** Produce two sequential records with one key and confirm the same partition and increasing offsets.

### 6. Disk usage grows because retention is too high

**Symptoms:** Kafka volume size grows steadily; `docker compose stats` shows container pressure; segments remain well beyond the intended period.

**Investigation commands:**

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-log-dirs.sh --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 --describe --topic-list orders
docker exec kafka-1 /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka-1:9092 --entity-type topics --entity-name orders --describe
```

**Likely cause:** High `retention.ms`, a large active segment, compacted topics, or replication multiplied the expected footprint.

**Remediation:** Set an approved retention value, such as one hour for a disposable training topic. Consider `segment.bytes` and `segment.ms` but do not delete volume files manually.

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka-1:9092 --entity-type topics --entity-name orders --alter --add-config retention.ms=3600000
```

**Post-fix verification:** Recheck effective configuration and log-dir sizes after cleanup and segment rollover:

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka-1:9092 --entity-type topics --entity-name orders --describe
```

### 7. A producer gets `NOT_ENOUGH_REPLICAS`

**Symptoms:** Writes fail with `NOT_ENOUGH_REPLICAS` or `NOT_ENOUGH_REPLICAS_AFTER_APPEND`; producer queues and latency may increase.

**Investigation commands:**

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --describe --under-replicated-partitions
docker exec kafka-1 /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka-1:9092 --entity-type topics --entity-name orders --describe
docker compose ps
```

**Likely cause:** Fewer than two replicas are in ISR for this lab's topic, while the producer uses `acks=all`.

**Remediation:** Restore unhealthy follower brokers and let replicas catch up. Do not lower `min.insync.replicas` merely to accept writes, because that weakens durability.

**Post-fix verification:** Produce with `acks=all` and confirm no URP:

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-producer-perf-test.sh --topic orders --num-records 1000 --record-size 256 --throughput 500 --producer-props bootstrap.servers=kafka-1:9092 acks=all
```

### 8. Partitions are distributed unevenly among brokers

**Symptoms:** Kafka UI or topic descriptions show one broker leading or storing disproportionately many partitions; broker CPU or disk is skewed.

**Investigation commands:**

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --describe
docker exec kafka-1 /opt/kafka/bin/kafka-log-dirs.sh --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 --describe
```

**Likely cause:** Recent broker failures, manual assignments, topic creation during imbalance, or preferred leaders not being elected.

**Remediation:** First run preferred leader election. If replica counts remain skewed, generate, review, and execute a `kafka-reassign-partitions` plan with an even broker distribution.

**Post-fix verification:**

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --describe --topic orders
docker exec kafka-1 /opt/kafka/bin/kafka-log-dirs.sh --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 --describe --topic-list orders
```

Leader and byte counts should be acceptably balanced. Exact equality is not always possible.

### 9. A consumer must replay messages from the beginning

**Symptoms:** Downstream state needs rebuilding, or records must be reprocessed after a corrected bug.

**Investigation commands:**

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:9092 --describe --group orders-training
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --describe --topic orders
```

**Likely cause:** An intentional replay request, not a broker fault. Confirm that required records still exist and have not expired.

**Remediation:** Stop every group member, preview a `--to-earliest` reset, and execute after review. Make downstream processing idempotent before replay.

**Post-fix verification:**

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:9092 --describe --group orders-training
docker exec kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka-1:9092 --topic orders --group orders-training --max-messages 5 --from-beginning --property print.offset=true
```

The consumer should read from the earliest retained offsets.

### 10. A topic was created with an incorrect replication factor

**Symptoms:** Topic describes show replication factor 1 or 2; a single broker failure creates unavailable or under-replicated partitions.

**Investigation commands:**

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --describe --topic orders
docker exec kafka-1 /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka-1:9092 --entity-type brokers --all --describe
```

**Likely cause:** The topic was explicitly created with the wrong replication factor, or an external tool bypassed the broker default.

**Remediation:** Kafka cannot alter replication factor with `kafka-topics --alter`. Generate or write a reassignment plan containing three broker IDs for every partition, validate it, and execute with `kafka-reassign-partitions`.

**Post-fix verification:**

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --describe --topic orders
```

Every `Replicas` entry must contain three brokers and, after catch-up, every ISR must also contain all three.

## Troubleshooting the lab

### Port conflicts

**Symptoms:** Compose reports that port 19092, 29092, 39092, or 8080 is already allocated.

```sh
docker ps --format "table {{.Names}}\t{{.Ports}}"
docker compose down
```

On Windows, identify the process:

```powershell
Get-NetTCPConnection -LocalPort 8080,19092,29092,39092 -State Listen
```

Stop the conflicting container/process, or change the host side of the Compose port mapping. If the external port changes, update that broker's advertised external listener too.

### Unhealthy brokers

```sh
docker compose ps
docker inspect -f "{{.State.Health.Status}}" kafka-1
docker logs --tail=300 kafka-1
```

Allow at least 30 seconds for first startup. Never assume `running` means Kafka is ready; inspect the health status and broker log.

### Listener configuration errors

**Symptoms:** A container can connect but a host application cannot, or a host appears as a DNS name inside a broker-to-broker error.

Container clients use:

```text
kafka-1:9092,kafka-2:9092,kafka-3:9092
```

Host clients use:

```text
localhost:19092,localhost:29092,localhost:39092
```

Check the advertised values:

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka-1:9092 --entity-type brokers --entity-name 1 --describe
```

The `INTERNAL` listener must be advertised with the Docker service name. The `EXTERNAL` listener must be advertised with `localhost` and the host port.

### Insufficient memory

**Symptoms:** OOM kills, restart loops, very slow health checks, or Docker Desktop reports resource pressure.

```sh
docker compose stats --no-stream kafka-1 kafka-2 kafka-3 kafka-ui
docker inspect -f "{{.State.OOMKilled}}" kafka-1
```

Stop unrelated Docker workloads and allocate more Docker Desktop memory. The lab caps each broker JVM at 512 MB; raising broker heap above its container limit is counterproductive.

### Volume permission issues

**Symptoms:** Kafka exits while formatting or opening its log directory under `/var/lib/kafka/data`.

```sh
docker logs --tail=300 kafka-1
docker volume inspect kafka-lab-kafka-1-data
```

Do not manually change files inside a Docker Desktop-managed named volume. If this is a disposable lab, stop it and recreate only the failing named volume after backing up anything needed:

```sh
docker compose down
docker volume rm kafka-lab-kafka-1-data
docker compose up -d
```

Removing one broker's volume from a running multi-node cluster is a recovery exercise and may trigger re-replication. `bash scripts/reset-lab.sh` is the simpler complete reset.

### Quorum is unavailable

**Symptoms:** Topic commands time out, no active controller is shown, or at least two combined nodes are down.

```sh
docker compose ps
docker exec kafka-1 /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server kafka-1:9092 describe --status
```

Bring controllers back one at a time until at least two voters are available. Never use different `KAFKA_CLUSTER_ID` values among voters or change node IDs against existing volumes.

## Kafka CLI cheat sheet

All examples execute from the host through the `kafka-1` container.

### `kafka-topics.sh`

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --list
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --create --topic demo --partitions 3 --replication-factor 3
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --describe --topic demo
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --alter --topic demo --partitions 6
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --delete --topic demo
```

### `kafka-console-producer.sh`

```sh
docker exec -it kafka-1 /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka-1:9092 --topic orders --producer-property acks=all
docker exec -it kafka-1 /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka-1:9092 --topic orders --property parse.key=true --property key.separator=:
```

### `kafka-console-consumer.sh`

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka-1:9092 --topic orders --from-beginning --max-messages 10
docker exec kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka-1:9092 --topic orders --partition 0 --offset earliest --timeout-ms 10000
```

### `kafka-consumer-groups.sh`

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:9092 --list
docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:9092 --describe --group orders-training
docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:9092 --describe --group orders-training --members --verbose
docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:9092 --delete --group orders-training
```

### `kafka-configs.sh`

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka-1:9092 --entity-type topics --entity-name orders --describe
docker exec kafka-1 /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka-1:9092 --entity-type topics --entity-name orders --alter --add-config retention.ms=3600000
docker exec kafka-1 /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka-1:9092 --entity-type topics --entity-name orders --alter --delete-config retention.ms
```

### `kafka-log-dirs.sh`

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-log-dirs.sh --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 --describe
docker exec kafka-1 /opt/kafka/bin/kafka-log-dirs.sh --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 --describe --broker-list 1,2,3 --topic-list orders
```

### `kafka-metadata-quorum.sh`

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server kafka-1:9092 describe --status
docker exec kafka-1 /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server kafka-1:9092 describe --replication
```

### `kafka-reassign-partitions.sh`

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-reassign-partitions.sh --bootstrap-server kafka-1:9092 --list
docker exec kafka-1 /opt/kafka/bin/kafka-reassign-partitions.sh --bootstrap-server kafka-1:9092 --verify --reassignment-json-file /tmp/reassignment.json
```

### `kafka-leader-election.sh`

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-leader-election.sh --bootstrap-server kafka-1:9092 --election-type PREFERRED --all-topic-partitions
```

### `kafka-producer-perf-test.sh`

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-producer-perf-test.sh --topic orders --num-records 10000 --record-size 256 --throughput 1000 --producer-props bootstrap.servers=kafka-1:9092 acks=all
```

### `kafka-consumer-perf-test.sh`

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-consumer-perf-test.sh --bootstrap-server kafka-1:9092 --topic orders --messages 10000 --timeout 120000 --group consumer-perf-cheat-sheet
```

### `kafka-dump-log.sh`

First list data files, then select a segment. Do not write to these files:

```sh
docker exec kafka-1 bash -lc 'find /var/lib/kafka/data -type f -name "*.log" | head'
docker exec kafka-1 /opt/kafka/bin/kafka-dump-log.sh --print-data-log --files /var/lib/kafka/data/orders-0/00000000000000000000.log
```

The exact segment name depends on the data generated in the lab. The dump is useful for low-level record inspection and offset-recovery learning.

## Practice launcher

`commands/kafka-practice.sh` wraps frequently used safe operations:

```sh
bash commands/kafka-practice.sh
bash commands/kafka-practice.sh health
bash commands/kafka-practice.sh create-orders
bash commands/kafka-practice.sh describe-orders
bash commands/kafka-practice.sh group-lag orders-training
```

PowerShell can run it through Git Bash or WSL:

```powershell
bash .\commands\kafka-practice.sh health
```

## Shell command notes for Windows PowerShell

Most single-line Docker commands are identical in PowerShell. Differences are:

- Linux/macOS `/dev/null` becomes `$null`, such as `2>$null` in shell redirection before piping to a host process.
- Bash line continuation uses `\`; PowerShell uses a backtick, or simply keep the displayed command on one line.
- Bash heredocs are replaced by PowerShell here-strings, demonstrated in the keyed-producer and reassignment examples.
- Shell scripts run with Git Bash, WSL, or by executing their underlying single-line Docker commands directly.
- PowerShell interpolation of `$(...)` and `${...}` should be avoided inside quoted command snippets. Use the literal Docker commands supplied above.

## First commands to run

1. Open a terminal in `apps\python_program\kafka-local-lab`.
2. Validate and start:

```sh
docker compose config
docker compose up -d
```

3. Wait for all services:

```sh
docker compose ps
```

4. Create and inspect `orders`:

```sh
bash scripts/create-topics.sh
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --describe --topic orders
```

5. Produce sample data and view it:

```sh
bash scripts/produce-sample-data.sh
docker exec kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka-1:9092 --topic orders --from-beginning --property print.key=true --property print.partition=true --property print.offset=true --property print.timestamp=true --timeout-ms 10000
```

6. Open [http://localhost:8080](http://localhost:8080).
