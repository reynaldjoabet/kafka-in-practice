# kafka-in-practice: Kafka Fundamentals

How records move from a producer, through replicated partition logs, to consumer groups, and how the KRaft controller quorum keeps the whole cluster agreeing on who leads what.

Companion document: [tiered-storage-and-security.md](tiered-storage-and-security.md).

## Contents

**Part 1: Foundations**

1. [The big picture](#1-the-big-picture)
2. [Topics, partitions, and the log](#2-topics-partitions-and-the-log)
3. [Replication and the ISR](#3-replication-and-the-isr)

**Part 2: Writing**

4. [Inside the producer](#4-inside-the-producer)
5. [Acks and idempotence](#5-acks-and-idempotence)
6. [Transactions](#6-transactions)

**Part 3: Reading**

7. [Inside the consumer](#7-inside-the-consumer)
8. [Consumer groups](#8-consumer-groups)
9. [The group coordinator](#9-the-group-coordinator)
10. [Rebalancing, old and new](#10-rebalancing-old-and-new)
11. [Share groups](#11-share-groups-queues-for-kafka)
12. [Delivery guarantees](#12-delivery-guarantees)

**Part 4: Control plane (KRaft)**

13. [Why KRaft exists](#13-why-kraft-exists)
14. [The Raft quorum](#14-the-raft-quorum)
15. [The active controller](#15-the-active-controller)
16. [Brokers in KRaft](#16-brokers-in-kraft)

**Part 5: Putting it together**

17. [A record's whole journey](#17-a-records-whole-journey)
18. [Configs that matter](#18-configs-that-matter)
19. [Where it lives in the code](#19-where-it-lives-in-the-code)

**Part 6: Under the hood**

20. [The record batch format](#20-the-record-batch-format)
21. [Inside a broker: how a request is processed](#21-inside-a-broker-how-a-request-is-processed)
22. [The storage engine: page cache, zero-copy, and durability](#22-the-storage-engine-page-cache-zero-copy-and-durability)

**Part 7: Designing with Kafka**

23. [Topic and partition design](#23-topic-and-partition-design)
24. [Keys, schemas, and message size](#24-keys-schemas-and-message-size)
25. [Reliability patterns](#25-reliability-patterns)
26. [Kafka Streams](#26-kafka-streams)
27. [Kafka Connect](#27-kafka-connect)
28. [Multiple clusters and disaster recovery](#28-multiple-clusters-and-disaster-recovery)

**Part 8: Running Kafka**

29. [Performance tuning](#29-performance-tuning)
30. [Operations runbook](#30-operations-runbook)
31. [Monitoring: the metrics that matter](#31-monitoring-the-metrics-that-matter)
32. [Troubleshooting playbook](#32-troubleshooting-playbook)
33. [Testing Kafka applications](#33-testing-kafka-applications)
34. [Anti-patterns](#34-anti-patterns)
35. [Self-test](#35-self-test)
36. [Where to go next](#36-where-to-go-next)

---

## The one idea behind everything

**Everything important in Kafka is an append-only log.**

| What | Log |
|---|---|
| Your data | a partition, e.g. `orders-0` |
| Consumer group state and committed offsets | `__consumer_offsets` |
| Transaction state | `__transaction_state` |
| Share group state | `__share_group_state` |
| Tiered storage segment metadata | `__remote_log_metadata` |
| The cluster's own metadata | `__cluster_metadata` (replicated with Raft) |

Once you see that, most of Kafka's design follows from three questions: *who appends to this log, who reads it, and what happens when the leader dies?*

---

# Part 1: Foundations

## 1. The big picture

A Kafka cluster has two kinds of server roles:

- **Brokers** store partitions and handle **Produce** requests (writes) and **Fetch** requests (reads). "Produce" and "Fetch" are the names of the two main request types in Kafka's network protocol. This is the **data plane**: it carries your actual records.
- **Controllers** form a small Raft quorum that owns the cluster metadata: which brokers are alive, which topics exist, and which replica leads each partition. This is the **control plane**. Controllers never see your records; they only decide who leads.

A single JVM can run either role or both (`process.roles=broker,controller`). Production clusters usually run 3 or 5 dedicated controllers.

Clients are smart. A producer or consumer asks any broker for metadata, learns which broker leads each partition, and then talks to those leaders directly. There is no proxy or load balancer in the path.

```mermaid
flowchart TB
    subgraph Q["KRaft controller quorum"]
        C1["C1 (active)"]
        C2["C2"]
        C3["C3"]
    end
    subgraph B1["Broker 1"]
        o0L["orders-0 LEADER"]
        o1F1["orders-1 follower"]
        o2F1["orders-2 follower"]
    end
    subgraph B2["Broker 2"]
        o0F2["orders-0 follower"]
        o1L["orders-1 LEADER"]
        o2F2["orders-2 follower"]
    end
    subgraph B3["Broker 3"]
        o0F3["orders-0 follower"]
        o1F3["orders-1 follower"]
        o2L["orders-2 LEADER"]
    end
    PA["Producer A"] -- "produce" --> B1
    PB["Producer B"] -- "produce" --> B2
    B3 -- "fetch" --> G["Consumer group billing<br/>consumer 1, consumer 2"]
    Q -- "brokers fetch the metadata log" --> B2
    B2 -- "register, heartbeat" --> Q
```

*One topic, three partitions, replication factor 3. Leadership is spread so each broker leads one partition. Producers and consumers talk only to leaders by default.*

### Vocabulary

| Term | What it is |
|---|---|
| **Record** | Key (optional), value, headers, timestamp. Immutable once written. |
| **Topic** | A named stream of records, like `orders`. Only a logical name; the real storage unit is the partition. |
| **Partition** | An ordered, append-only log. The unit of ordering, parallelism, and replication. |
| **Offset** | A record's position in its partition: 0, 1, 2… Never reused. |
| **Replica** | A copy of a partition on some broker. One replica is the leader; the others are followers. |
| **Broker** | A server that hosts replicas and answers client requests. |
| **Controller** | A member of the KRaft quorum. One is *active* and makes all metadata decisions. |
| **Consumer group** | A set of consumers sharing a `group.id` that split a topic's partitions between them. |

## 2. Topics, partitions, and the log

A partition is a file-backed append-only log. New records always go on the end and get the next offset. Nothing in the middle changes. This gives Kafka two famous properties:

- Writes are sequential disk appends, which are fast.
- Readers keep their own position and can re-read history at will. The broker does not track "delivered" state per message for normal consumers.

```
orders-0 on the leader

  segment ...00.log         segment ...04.log         active segment ...08.log
 ┌────┬────┬────┬────┐    ┌────┬────┬────┬────┐    ┌────┬────┬╌╌╌╌┬╌╌╌╌┐
 │ 0  │ 1  │ 2  │ 3  │    │ 4  │ 5  │ 6  │ 7  │    │ 8  │ 9  │ 10 │ 11 │  ──► append
 └────┴────┴────┴────┘    └────┴────┴────┴────┘    └────┴────┴╌╌╌╌┴╌╌╌╌┘
 ▲                                  ▲                        ▲          ▲
 log start 0                billing committed: 6     high watermark 10  log end 12
 (retention trims here)     (next offset to read)    (readable below)   (next write)

 solid  = replicated to every in-sync replica, visible to consumers
 dashed = on the leader only, not yet visible
```

A partition is a sequence of **segment files**. Each segment is named after its first offset and has two sparse indexes beside it:

- `.index`: offset → byte position in the file
- `.timeindex`: timestamp → offset

Only the active segment is written to. Retention deletes whole old segments.

### Four offsets you should know

- **Log start offset**: the oldest offset still on disk. It moves forward when retention deletes segments (`retention.ms`, default 7 days, or `retention.bytes`).
- **Committed offset** (per consumer group): where that group will resume. It is stored by the group coordinator, not in the partition.
- **High watermark (HW)**: the first offset not yet copied to every in-sync replica. Consumers can only read below it, which is why a record you have seen can never "disappear" after a leader change.
- **Log end offset (LEO)**: the offset the next appended record will get.

### Keys decide the partition

Ordering is guaranteed only **within** a partition. If you need all events for `customer-42` in order, give them the key `customer-42`. The default partitioner hashes the key (murmur2) modulo the partition count, so the same key always lands on the same partition.

Adding partitions later changes that mapping, so keys move to different partitions. That is why people over-provision partitions up front.

### Two cleanup policies

| Policy | Behaviour | Typical use |
|---|---|---|
| `cleanup.policy=delete` (default) | Old segments are dropped after time or size limits. | A time-bounded event history. |
| `cleanup.policy=compact` | The log cleaner keeps at least the latest record per key and removes older ones. A record with a `null` value (a **tombstone**) deletes the key. | A changelog table. `__consumer_offsets` is compacted. |

**Tiered storage** (KIP-405) adds a third dimension: closed segments can be copied to object storage like S3, so the broker keeps only a hot tail on local disk while consumers can still read the full history. See the companion README.

## 3. Replication and the ISR

Every partition has `replication.factor` replicas (3 is the usual choice). The leader accepts writes. Followers replicate by sending the leader ordinary **Fetch** requests, the same API consumers use, with their current LEO as the fetch offset. That fetch offset tells the leader how far each follower has got.

```
                                          high watermark 11 = lowest LEO in the ISR
                                                     ┆
Broker 1 · leader    [0][1][2][3][4][5][6][7][8][9][10][11]   LEO 12
Broker 2 · follower  [0][1][2][3][4][5][6][7][8][9][10]       Fetch(offset=11)   in ISR
Broker 3 · follower  [0][1][2][3][4][5]                       Fetch(offset=6)    OUT of ISR
                                                     ┆
ISR = {1, 2}. Broker 3 was removed after not catching up for replica.lag.time.max.ms (30 s).
```

The leader advances the **high watermark** to the lowest LEO among in-sync replicas. A slow follower does not hold the HW back forever: once it lags longer than `replica.lag.time.max.ms`, the leader asks the controller to shrink the ISR, and the HW moves on without it. When it catches up, it is added back.

### The in-sync replica set (ISR)

The **ISR** is the set of replicas that are caught up enough to take over as leader without losing committed data.

- The controller stores the ISR in metadata, and only the controller can change it. The leader asks for changes with `AlterPartition` requests.
- When a leader dies, the controller picks a new leader from the ISR.
- `min.insync.replicas` (often set to 2) is the safety floor. A write with `acks=all` fails with `NotEnoughReplicas` if the ISR is smaller than this, instead of being accepted with too few copies.

### Leader epochs

Every leader change bumps the partition's **leader epoch**. Each replica records which epoch began at which offset (the `leader-epoch-checkpoint` file).

When a former leader comes back, it asks the new leader where its epoch ended and truncates any records past that point. Truncation can only remove records above the old high watermark, which no consumer was ever allowed to see.

### Newer refinements

- **Eligible Leader Replicas** (KIP-966) track replicas that dropped out of the ISR but are still known to hold all committed data. This makes leader election safer when `min.insync.replicas` is set.
- `unclean.leader.election.enable=false` (the default) means Kafka would rather keep a partition offline than elect a replica that is missing committed data.

---

# Part 2: Writing

## 4. Inside the producer

`producer.send(record)` does not send anything. It puts the record into a buffer and returns a `Future` right away. A separate I/O thread called the **Sender** drains that buffer into network requests. This split is why a single producer can push hundreds of thousands of records per second.

```mermaid
flowchart LR
    subgraph App["Application thread"]
        S["send()<br/>returns Future"] --> Ser["Serializer<br/>key, value → bytes"]
        Ser --> Part["Partitioner<br/>key hash or sticky"]
        Part --> Acc
        subgraph Acc["RecordAccumulator (per-partition batch queues)"]
            q0["orders-0: [ready][ ][ ]"]
            q1["orders-1: [ ]"]
            q2["orders-2: [ready][ ]"]
        end
    end
    subgraph IO["Sender I/O thread"]
        Snd["Sender<br/>drains ready batches"]
    end
    Acc -- "ready batches" --> Snd
    Snd -- "1 ProduceRequest per broker" --> B1["Broker 1"]
    Snd --> B2["Broker 2"]
    Snd --> B3["Broker 3"]
    B1 -. "ack or error completes the Future, runs your callback" .-> S
```

Records are grouped into **per-partition batches**. A batch is sent when it fills to `batch.size` (16 KB) or has waited `linger.ms` (5 ms by default since Kafka 4.0). The Sender merges all ready batches bound for the same broker into one request, so larger batches mean fewer requests and better compression.

Key behaviours:

- **Partitioner.** With a key: `murmur2(key) % partitions`. Without a key: the built-in partitioner sticks to one partition until a batch fills, then switches, preferring partitions on less-loaded brokers. Sticking avoids sending many tiny batches.
- **Memory.** The accumulator has `buffer.memory` (32 MB). When it is full, `send()` blocks up to `max.block.ms`. That backpressure is how a fast producer learns the cluster is slow.
- **Compression** (`compression.type`: `lz4`, `zstd`, `snappy`, `gzip`) is applied per batch. Brokers store the batch compressed and consumers decompress it, so the broker rarely does the work.
- **Retries.** Retriable errors (leader moved, timeout) are retried until `delivery.timeout.ms` (2 minutes) runs out. On `NotLeaderOrFollower`, the producer refreshes metadata and resends to the new leader.

## 5. Acks and idempotence

`acks` decides when the leader responds. It is the main knob between latency and durability.

| `acks` | Leader replies when… | Can acknowledged data be lost? |
|---|---|---|
| `0` | Never. The producer does not wait. | **Yes.** Anything can be lost. |
| `1` | The leader has written it to its own log. | **Yes**, if the leader dies before followers copy it. |
| `all` (default) | Every replica in the ISR has it, and ISR ≥ `min.insync.replicas`. | **No**, unless more than (replication factor − `min.insync.replicas`) replicas fail together. |

### Idempotence: retries without duplicates

Retries create a classic problem: the broker wrote the batch, the acknowledgement got lost, the producer retries, and now the record exists twice.

Idempotence (on by default since Kafka 3.0) fixes this:

1. The producer gets a **producer ID** (PID) from the broker.
2. It numbers every batch per partition with a **sequence number**.
3. The partition leader remembers the last few sequences per producer ID (in `ProducerStateManager`) and drops repeats.

```mermaid
sequenceDiagram
    participant P as Producer (PID 7)
    participant L as Leader of orders-0
    P->>L: Produce batch seq=5
    L->>L: append at offsets 40-49, remember PID 7 seq 5
    L--xP: ack lost on the network
    P->>L: retry Produce batch seq=5
    L->>L: seq 5 already written for PID 7
    L-->>P: success, offset 40 (no second copy)
    P->>L: Produce batch seq=6
    L-->>P: success, offset 50
```

The broker deduplicates by (producer ID, partition, sequence). Sequence gaps are rejected too, so ordering survives retries even with up to 5 requests in flight per connection.

Idempotence covers **one producer session writing to one partition**. If your application restarts, it gets a new producer ID. Surviving restarts and writing atomically across partitions is what transactions are for.

## 6. Transactions

A transaction lets a producer write to several partitions, and commit consumer offsets, as one atomic unit: either everything becomes visible or nothing does. This is the basis of exactly-once stream processing: read from topic A, write to topic B, and commit the input offset, all atomically.

### Fencing zombies

You set `transactional.id` to a stable name. The **transaction coordinator** maps it to a producer ID and an **epoch**. The coordinator is a role on one broker, chosen by hashing the `transactional.id` onto a partition of `__transaction_state`.

When a new instance starts with the same `transactional.id`, the epoch is bumped and the old instance is fenced. Any write from a "zombie" with the stale epoch is rejected.

### The commit protocol

```mermaid
sequenceDiagram
    participant P as Producer
    participant TC as Transaction coordinator
    participant TS as __transaction_state
    participant A as orders-0 leader
    participant B as payments-2 leader
    P->>TC: InitProducerId(transactional.id=checkout)
    TC->>TS: PID 7, epoch 3 (fences epoch 2)
    TC-->>P: PID 7, epoch 3
    P->>P: beginTransaction()
    P->>TC: AddPartitionsToTxn(orders-0, payments-2)
    TC->>TS: Ongoing, partitions={orders-0, payments-2}
    P->>A: Produce (transactional batch)
    P->>B: Produce (transactional batch)
    P->>TC: commitTransaction() → EndTxn(COMMIT)
    TC->>TS: PrepareCommit (the decision point)
    TC->>A: WriteTxnMarkers(COMMIT)
    TC->>B: WriteTxnMarkers(COMMIT)
    TC->>TS: CompleteCommit
```

This is a two-phase commit with Kafka logs as the durable state. Once `PrepareCommit` is in `__transaction_state`, the commit will finish even if the coordinator crashes, because the next coordinator replays the log. Commit markers are special control records written into each data partition.

### Reading transactional data

- `isolation.level=read_committed`: consumers read only up to the **last stable offset** (LSO), the first offset that belongs to a transaction still open. Records from aborted transactions are filtered out.
- `isolation.level=read_uncommitted` (the default): consumers read up to the high watermark and see everything, including aborted data.

Kafka 4.x also includes **KIP-890** ("transactions server-side defense"): the epoch is bumped on every transaction, and the broker adds partitions to the transaction implicitly. This closes several hanging-transaction bugs.

---

# Part 3: Reading

## 7. Inside the consumer

Consumers **pull**. The broker never pushes records. Your code calls `poll()` in a loop; the consumer returns records it has already fetched and fetches more in the background. Pulling lets each consumer go at its own pace and makes replay trivial: to re-read, seek to an older offset.

```mermaid
flowchart LR
    subgraph You["Your thread"]
        L["while (running) {<br/>  recs = poll(100ms)<br/>  process(recs)<br/>  commitAsync()<br/>}"]
    end
    subgraph KC["KafkaConsumer"]
        Buf["fetch buffer<br/>records already fetched"]
        BG["background thread<br/>fetch · heartbeat · commit"]
    end
    L -- "poll()" --> Buf
    BG -- "Fetch" --> PL["Partition leaders<br/>(any broker)"]
    PL -- "records" --> Buf
    BG <-- "heartbeat, OffsetCommit" --> GC["Group coordinator<br/>(one specific broker)"]
```

Two different brokers matter to a consumer:

- **Partition leaders** serve the data.
- **The group coordinator** handles membership, assignment, and committed offsets.

In the newer `AsyncKafkaConsumer` (used with `group.protocol=consumer`), network work runs on a background thread as drawn. The older `ClassicKafkaConsumer` does it inside `poll()`.

### Position vs committed offset

A consumer tracks two numbers per partition:

- **Position**: the next offset `poll()` will return. It lives in memory.
- **Committed offset**: what it has told the coordinator it has finished. It is durable, and it is where the group resumes after a crash or reassignment.

The gap between them is what you might reprocess after a failure.

### Commit and liveness settings

- `enable.auto.commit=true` (default) commits the position every 5 s during `poll()`. Simple, but a crash can replay up to 5 s of records, or skip records if you hand them to another thread for processing.
- Manual `commitSync()` / `commitAsync()` after processing gives you at-least-once with tight control.
- `auto.offset.reset` (`latest` by default, or `earliest`) applies only when the group has no committed offset, or the committed offset points to data that retention already deleted.
- `max.poll.interval.ms` (5 min) is a liveness check on **your code**. If you do not call `poll()` within it, the consumer leaves the group and its partitions are reassigned.

Consumers can also read from a follower in the same rack (`client.rack` on the client plus `replica.selector.class` on the broker), which saves cross-zone network cost.

## 8. Consumer groups

A consumer group is how Kafka scales reading. All consumers that start with the same `group.id` form one group. Kafka divides the topic's partitions among them so that **each partition is read by exactly one member of the group**. Different groups are independent: each gets the full stream and keeps its own offsets.

This section covers what a group is and how it behaves from your application's point of view. Section 9 covers the broker component that manages groups, and section 10 covers the rebalance protocols in detail.

### 8.1 Queue and publish/subscribe in one model

Messaging systems traditionally offer two models:

- **Queue:** each message goes to *one* of several workers. Work is shared.
- **Publish/subscribe:** each message goes to *every* subscriber.

Consumer groups give you both, depending on how you set `group.id`:

```mermaid
flowchart LR
    T["topic orders<br/>P0 P1 P2 P3"]
    subgraph G1["group billing (acts as a queue)"]
        B1["consumer 1<br/>P0, P1"]
        B2["consumer 2<br/>P2, P3"]
    end
    subgraph G2["group analytics"]
        A1["consumer 1<br/>P0 P1 P2 P3"]
    end
    subgraph G3["group audit"]
        U1["consumer 1<br/>P0 P1 P2 P3"]
    end
    T -- "work split inside the group" --> G1
    T -- "full copy" --> G2
    T -- "full copy" --> G3
```

- **Inside a group**, partitions are split between members, so each record is processed by one member. That is the queue behaviour.
- **Across groups**, every group receives every record. That is the publish/subscribe behaviour.
- Reading does not remove anything. The records stay in the log until retention deletes them, so a fourth group created next week can still read from the beginning.

### 8.2 The assignment rules

1. Within a group, a partition is owned by **at most one** member at a time. This is what keeps per-partition ordering intact: one partition, one reader, in order.
2. One member can own **many** partitions.
3. A member that owns no partitions sits **idle**.
4. Different groups never affect each other's assignments.

```
topic orders · 6 partitions:   P0  P1  P2  P3  P4  P5     (each group reads all of these)

group billing · 3 members
  ┌ consumer 1 ┐ ┌ consumer 2 ┐ ┌ consumer 3 ┐
  │  P0   P1   │ │  P2   P3   │ │  P4   P5   │
  └────────────┘ └────────────┘ └────────────┘

group analytics · 1 member
  ┌ consumer 1 ─────────────────┐
  │  P0  P1  P2  P3  P4  P5     │
  └─────────────────────────────┘

group search · 7 members, so one has nothing to do
  [c1·P0] [c2·P1] [c3·P2] [c4·P3] [c5·P4] [c6·P5] [c7·idle]
```

### 8.3 Scaling a group up and down

The number of partitions is the ceiling on parallelism within one group. Here is the same six-partition topic as a group grows:

```
members │ assignment                                        │ partitions per member
────────┼───────────────────────────────────────────────────┼──────────────────────────────
   1    │ c1:[P0 P1 P2 P3 P4 P5]                            │ 6
   2    │ c1:[P0 P1 P2]  c2:[P3 P4 P5]                      │ 3 3
   3    │ c1:[P0 P1]  c2:[P2 P3]  c3:[P4 P5]                │ 2 2 2
   4    │ c1:[P0 P1]  c2:[P2 P3]  c3:[P4]  c4:[P5]          │ 2 2 1 1   ← uneven
   6    │ c1:[P0] c2:[P1] c3:[P2] c4:[P3] c5:[P4] c6:[P5]   │ 1 each    ← maximum parallelism
   7    │ same as 6, plus c7:[ ]                            │ one idle  ← no gain
```

What this means in practice:

- **Choose the partition count for the largest group you expect.** With 6 partitions, no group can ever use more than 6 consumers.
- **Partition counts with many divisors** (6, 12, 24, 60) spread evenly over more group sizes. With 4 consumers on 6 partitions, two consumers do twice the work of the other two.
- **Idle members are still useful** as hot standbys: if an active member dies, a standby takes over its partitions in the next rebalance.
- Each consumer handles all its partitions in one `poll()` loop. If one consumer is too slow for its share, add members (up to the partition count) or speed up processing.

### 8.4 Each group has its own offsets

A group's progress is a set of **committed offsets**, one per partition it reads. They are stored in `__consumer_offsets` under the key *(group, topic, partition)*. Two groups reading the same partition have completely separate positions:

```
orders-0   [0][1][2][3][4][5][6][7][8][9][10][11][12][13][14][15][16][17][18][19]
                       ▲                                    ▲                  ▲
                analytics committed 4             billing committed 13     log end 20

  billing   lag = 20 - 13 = 7      (nearly caught up)
  analytics lag = 20 - 4  = 16     (behind, maybe slower processing or newly started)
```

- **Committed offset** means "the next offset this group should read". After processing offset 12, the consumer commits 13.
- **Consumer lag** is log end offset minus committed offset: how many records the group still has to process. Lag is the most important metric for a consumer group. Steadily growing lag means the group cannot keep up.
- **Where a new group starts** is set by `auto.offset.reset`: `latest` (the default) starts at the end and skips history, while `earliest` starts at the log start offset.
- **Offsets of an empty group expire.** When a group has no members for `offsets.retention.minutes` (7 days by default), its committed offsets are deleted. If it comes back later, `auto.offset.reset` decides where it starts again.

You can see all of this with the CLI:

```bash
bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group billing
```

```
GROUP    TOPIC   PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID       HOST         CLIENT-ID
billing  orders  0          13              20              7    consumer-1-4f2a…  /10.0.0.11   consumer-1
billing  orders  1          88              88              0    consumer-1-4f2a…  /10.0.0.11   consumer-1
billing  orders  2          41              57              16   consumer-2-9c1d…  /10.0.0.12   consumer-2
```

`CURRENT-OFFSET` is the committed offset, not the consumer's in-memory position.

### 8.5 How a group knows a member is alive

A group has to tell the difference between a member that is healthy, one that has crashed, and one whose processing is stuck. It uses **two independent checks**:

```mermaid
flowchart TB
    subgraph Client["Consumer process"]
        HB["Heartbeat (background)<br/>every few seconds"]
        Loop["Your poll() loop"]
    end
    GC["Group coordinator"]
    HB -- "I'm alive" --> GC
    GC -- "no heartbeat for the session timeout<br/>→ member removed (process crashed or network lost)" --> X1["rebalance"]
    Loop -- "poll() not called within max.poll.interval.ms<br/>→ consumer leaves the group itself (processing stuck)" --> X2["rebalance"]
```

| Check | Detects | Classic protocol | Consumer protocol (KIP-848) |
|---|---|---|---|
| Heartbeats + session timeout | Process crash, network loss | Client sets `heartbeat.interval.ms` (3 s) and `session.timeout.ms` (45 s) | Broker sets `group.consumer.heartbeat.interval.ms` (5 s) and `group.consumer.session.timeout.ms` (45 s) |
| Poll interval | Your code being too slow or stuck | `max.poll.interval.ms` (5 min) | `max.poll.interval.ms` (5 min) |

How the common situations play out:

| Situation | What happens | How long until partitions move |
|---|---|---|
| `consumer.close()` | Member leaves the group at once | Immediately |
| Process killed (`kill -9`), machine dies | Heartbeats stop | Up to the session timeout (45 s) |
| One batch takes 10 minutes to process | Heartbeats continue, but `poll()` is late | After `max.poll.interval.ms` (5 min); the member is removed, and its next commit fails |
| Rolling restart with static membership | Member returns within the session timeout with the same `group.instance.id` | No reassignment at all |

**Always close consumers on shutdown** (`consumer.close()`, for example from a shutdown hook). Otherwise every deploy makes the group wait 45 seconds for the dead members to time out, with their partitions unprocessed in the meantime.

### 8.6 What triggers a rebalance

Partitions are redistributed when the inputs to the assignment change:

- A member **joins** (new instance, scale-up).
- A member **leaves** cleanly (`close()`) or is **removed** (session timeout, poll interval exceeded).
- A member changes its **subscription** (for example, a regex subscription starts matching a new topic).
- A subscribed topic **gains partitions**.
- A subscribed topic is **created or deleted**.

That redistribution is a **rebalance**, and it is run by the **group coordinator** (section 9). With the classic protocol every member takes part in every rebalance; with the new consumer protocol only members whose partitions change are affected (section 10).

### 8.7 What your code sees during a rebalance

Your application is told about ownership changes through a `ConsumerRebalanceListener`. These callbacks run inside `poll()` on your thread:

```mermaid
sequenceDiagram
    participant App as Your code
    participant K as KafkaConsumer
    participant GC as Group coordinator
    GC-->>K: P2 must move to another member
    K->>App: onPartitionsRevoked([P2])
    App->>K: commitSync() for work done on P2
    App->>App: flush or close per-partition state for P2
    K-->>GC: P2 released
    GC-->>K: you now own P4
    K->>App: onPartitionsAssigned([P4])
    App->>App: load state for P4, or seek() somewhere specific
    Note over App,K: if the member was kicked out (session timeout),<br/>onPartitionsLost is called instead of onPartitionsRevoked
```

| Callback | When | What to do |
|---|---|---|
| `onPartitionsRevoked` | You are giving partitions up in an orderly way | Commit offsets for what you have processed, flush state. This is your last chance before another member continues from your last commit. |
| `onPartitionsAssigned` | You received partitions | Initialise per-partition state, optionally `seek()` |
| `onPartitionsLost` | You were already removed from the group; another member may own these partitions now | Clean up, but **do not commit**. The commit would be rejected, and it could overwrite the new owner's progress. |

In Scala:

```scala
import java.util
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.common.TopicPartition

consumer.subscribe(util.List.of("orders"), new ConsumerRebalanceListener {
  override def onPartitionsRevoked(parts: util.Collection[TopicPartition]): Unit =
    consumer.commitSync()                 // save progress before someone else takes over

  override def onPartitionsAssigned(parts: util.Collection[TopicPartition]): Unit =
    parts.forEach(tp => println(s"now reading $tp"))

  override def onPartitionsLost(parts: util.Collection[TopicPartition]): Unit =
    println(s"lost $parts without a clean handoff; not committing")
})
```

`commitSync()` with no arguments commits the current position of every owned partition. That is correct only if you have finished processing everything `poll()` returned. If you process asynchronously, commit the offsets you have actually completed instead.

### 8.8 Assignment strategies

The **assignor** decides which member gets which partition. Which one runs depends on the protocol:

| Protocol | Where the assignor runs | Built-in options | Default |
|---|---|---|---|
| Classic | In the client (the group leader member) | `range`, `roundrobin`, `sticky`, `cooperative-sticky` | `partition.assignment.strategy` = Range, then CooperativeSticky |
| Consumer (KIP-848) | On the broker | `uniform`, `range` | `uniform` (server config `group.consumer.assignors`) |

Two members, subscribed to topics T1 and T2, each with 3 partitions:

```
              member M0                    member M1
range         T1P0 T1P1 T2P0 T2P1          T1P2 T2P2          4 / 2, but co-partitioned
round-robin   T1P0 T1P2 T2P1               T1P1 T2P0 T2P2     3 / 3, partitions interleaved
uniform       3 partitions                 3 partitions       balanced, and sticky across changes
```

- **Range** gives each member the *same partition numbers* in every topic (M0 gets partition 0 and 1 of both T1 and T2). This is called **co-partitioning**. It matters for joins: if both topics are keyed by `customer-id`, the same customer is in the same partition number of both, so one member sees both sides of the join. The cost is imbalance.
- **Round-robin** and **uniform** balance the load. Uniform also tries to keep existing assignments when it can.

**Stickiness** decides how much moves when membership changes. Three members, then M2 leaves:

```
before          M0:[P0 P1]        M1:[P2 P3]        M2:[P4 P5]

not sticky      M0:[P0 P1 P2]     M1:[P3 P4 P5]                      P2 and P3 moved too
sticky          M0:[P0 P1 P4]     M1:[P2 P3 P5]                      only M2's partitions moved
```

Every partition that moves has to be committed by its old owner and picked up by its new owner, so moving fewer partitions means a shorter pause and less reprocessing.

### 8.9 Group states

The coordinator reports a state for each group, shown by `kafka-consumer-groups.sh --describe --state`. For groups using the consumer protocol (the states are defined in `ConsumerGroup.java`):

```mermaid
stateDiagram-v2
    direction LR
    [*] --> Empty
    Empty --> Assigning: first member joins
    Assigning --> Reconciling: new target assignment computed
    Reconciling --> Stable: every member reached the target
    Stable --> Assigning: member joins or leaves, subscription or partitions change
    Reconciling --> Assigning: another change arrives mid-way
    Stable --> Empty: last member leaves
    Empty --> Dead: offsets expired or group deleted
    Dead --> [*]
```

| State | Meaning |
|---|---|
| `Empty` | No members, but committed offsets still exist |
| `Assigning` | Membership changed and a new target assignment is being computed |
| `Reconciling` | Members are moving partitions toward the new target |
| `Stable` | Every member has its target partitions |
| `Dead` | No members and no metadata; the group is being removed |

Classic groups have different states: `Empty`, `PreparingRebalance` (waiting for every member to rejoin), `CompletingRebalance` (waiting for the leader's assignment), `Stable`, and `Dead`.

### 8.10 `subscribe()` vs `assign()`

You do not have to use group management at all.

| | `subscribe(topics)` | `assign(partitions)` |
|---|---|---|
| Who picks partitions | The group's assignor | You, explicitly |
| Rebalances, failover | Yes, automatic | None. If this process dies, nobody takes over its partitions. |
| Needs `group.id` | Yes | Only if you want to commit offsets to Kafka |
| Typical use | Almost all applications | Tools, replays, tests, or frameworks that manage their own placement |

### 8.11 Common pitfalls

- **Rebalance loops from slow processing.** A batch takes longer than `max.poll.interval.ms`, the member is removed, its partitions move, the new owner hits the same slow records, and so on. Fix it by lowering `max.poll.records` (default 500), speeding up processing, or raising `max.poll.interval.ms`.
- **More consumers than partitions.** The extra consumers do nothing. Check the partition count before scaling out.
- **Hot partitions.** One key (for example, one huge customer) sends most of the traffic to one partition. Only one member can read it, so adding consumers does not help. Spread the key, or split the work after reading.
- **Committing too early with worker threads.** If `poll()` hands records to a thread pool and auto-commit is on, offsets can be committed before processing finishes, and a crash then loses those records. Disable auto-commit and commit only completed offsets.
- **Not closing consumers.** Each deploy then pauses the affected partitions for the full session timeout.
- **Reusing a `group.id` by accident.** Two unrelated applications with the same `group.id` split the partitions between them, and each sees only part of the data.

### 8.12 Managing groups from the command line

```bash
# list all groups
bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list

# offsets and lag per partition (section 8.4)
bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group billing

# members and how many partitions each one owns
bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group billing --members

# group state, coordinator, assignor
bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group billing --state

# rewind the group to the beginning (the group must have no active members)
bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group billing \
  --topic orders --reset-offsets --to-earliest --dry-run     # preview
bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group billing \
  --topic orders --reset-offsets --to-earliest --execute     # apply

# other reset targets: --to-latest, --to-offset 42, --shift-by -100,
#                      --to-datetime 2026-10-01T00:00:00.000, --by-duration PT1H

# delete a group and its committed offsets (no active members allowed)
bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --delete --group billing
```

The next two sections go one level down: section 9 covers how the group coordinator is found and how it stores group state, and section 10 shows the message exchanges of a rebalance under both protocols.

## 9. The group coordinator

The group coordinator is not a separate server. It is a component running inside **every** broker. Each broker is responsible for the groups whose state lives in the `__consumer_offsets` partitions it leads. That internal topic has 50 partitions by default (`offsets.topic.num.partitions`).

### Finding the coordinator

```mermaid
flowchart LR
    G["group.id = billing"] -- "hash" --> H["abs(hashCode) % 50 = 9"]
    H -- "partition" --> P["__consumer_offsets-9"]
    P -- "leader of" --> B["Broker 2<br/>= billing's coordinator"]
```

The client asks any broker `FindCoordinator(key="billing")` and gets Broker 2 back. Every heartbeat, join, and offset commit for that group then goes to Broker 2.

The real computation is `Utils.abs(groupId.hashCode()) % numPartitions` in `GroupCoordinatorService.partitionFor`. For `"billing"` it really is 9.

If Broker 2 dies, another replica of `__consumer_offsets-9` becomes leader, and that broker becomes the coordinator.

### A coordinator is a state machine over a log

This is the most useful mental model in modern Kafka:

1. The coordinator keeps group state (members, subscriptions, assignments, committed offsets) in memory.
2. Every change is first written as a record to its `__consumer_offsets` partition.
3. The request is answered only after that record is committed (replicated to the ISR).
4. If the broker fails, the new leader of that partition reads the partition from the start and rebuilds exactly the same state.

The KRaft controller works the same way over `__cluster_metadata`:

```mermaid
flowchart LR
    subgraph Row1["Group coordinator"]
        R1["Heartbeat,<br/>OffsetCommit"] --> S1["GroupCoordinatorShard<br/>groups + offsets in memory"]
        S1 -- "records" --> L1["__consumer_offsets-9"]
        L1 -- "ISR replication" --> F1["2 follower brokers"]
        L1 -. "replay on failover" .-> S1
    end
    subgraph Row2["KRaft controller"]
        R2["CreateTopics,<br/>BrokerHeartbeat"] --> S2["QuorumController<br/>cluster metadata in memory"]
        S2 -- "records" --> L2["__cluster_metadata"]
        L2 -- "Raft" --> F2["controllers + all brokers"]
    end
```

The generic engine behind the first row is `CoordinatorRuntime` in `coordinator-common`. The group, share, and (being migrated) transaction coordinators all run on it.

## 10. Rebalancing, old and new

Kafka has two consumer group protocols, chosen on the client with `group.protocol`. The default in this tree is still `classic`. The new `consumer` protocol (KIP-848, production-ready since Kafka 4.0) is the future, and the difference is worth understanding.

### Classic protocol: a client leader assigns, with a global barrier

```mermaid
sequenceDiagram
    participant A as Consumer A (leader)
    participant B as Consumer B
    participant C as Consumer C (new)
    participant GC as Group coordinator
    C->>GC: JoinGroup
    GC-->>A: heartbeat says REBALANCE_IN_PROGRESS
    GC-->>B: heartbeat says REBALANCE_IN_PROGRESS
    Note over A,B: eager mode: revoke ALL partitions, stop processing
    A->>GC: JoinGroup (subscriptions)
    B->>GC: JoinGroup (subscriptions)
    Note over GC: wait until every member has joined (the barrier)
    GC-->>A: you are leader + all members' subscriptions
    GC-->>B: JoinGroup response (generation 8)
    GC-->>C: JoinGroup response (generation 8)
    A->>A: run assignor (range / roundrobin / cooperative-sticky)
    A->>GC: SyncGroup(full assignment)
    B->>GC: SyncGroup(empty)
    C->>GC: SyncGroup(empty)
    GC-->>A: your partitions
    GC-->>B: your partitions
    GC-->>C: your partitions
```

Everyone must rejoin, and with eager assignors everyone stops processing. The **cooperative-sticky** assignor softens this by revoking only partitions that actually move, at the cost of a second rebalance round. Either way, one slow member delays the whole group.

### New protocol (KIP-848): the broker assigns, each member moves alone

There is no JoinGroup or SyncGroup. Each member sends a `ConsumerGroupHeartbeat` every few seconds (interval set by the broker, 5 s by default) carrying its subscription. The coordinator runs the assignor itself (`uniform` or `range`, server-side) and then steers each member individually toward its target.

```mermaid
sequenceDiagram
    participant A as Consumer A (has P0,P1,P2)
    participant B as Consumer B (has P3,P4,P5)
    participant GC as Group coordinator
    participant C as Consumer C (new)
    C->>GC: Heartbeat(epoch 0, subscribe orders)
    Note over GC: group epoch 7 → 8<br/>new target: A={P0,P1} B={P3,P4} C={P2,P5}
    GC-->>C: member epoch 8, assigned {} for now
    A->>GC: Heartbeat
    GC-->>A: please give up P2 (keep P0,P1)
    B->>GC: Heartbeat
    GC-->>B: please give up P5 (keep P3,P4)
    Note over A,B: keep processing the partitions they keep
    A->>GC: Heartbeat(owned P0,P1)
    GC-->>A: member epoch 8, done
    GC-->>C: P2 is free, take it
    B->>GC: Heartbeat(owned P3,P4)
    GC-->>C: P5 is free, take it
```

Only the partitions that move are paused. A partition is handed to its new owner only after the old owner confirms it let go. No member waits for any other member to rejoin.

The coordinator tracks three epochs to make this safe:

- **Group epoch**: bumped whenever membership or subscriptions change.
- **Target assignment epoch**: the group epoch the current target assignment was computed for.
- **Member epoch**: how far each member has reconciled. Offset commits carry it, so a member that is behind (a zombie) cannot commit for partitions it no longer owns.

### Side by side

| | Classic | Consumer (KIP-848) |
|---|---|---|
| Who assigns | Client-side assignor on the leader member | Broker-side assignor |
| Coordination | Global sync barrier | Each member reconciles independently |
| Cost of a change | Grows with group size | Incremental by design |
| Client complexity | Thick clients, logic in every language | Thin clients, one implementation on the broker |

**Static membership** (`group.instance.id`) works with both: a consumer that restarts with the same instance id within the session timeout gets its old partitions back without triggering a rebalance.

Kafka Streams has its own analogous protocol, the **streams group** (KIP-1071), which moves task assignment to the broker in the same way.

## 11. Share groups (queues for Kafka)

Consumer groups give each partition to one member, so you can never have more active readers than partitions, and there is no per-record acknowledgement. **Share groups** (KIP-932) add queue semantics on top of the same topics:

- Many members can read **the same partition** at once.
- Records are **acquired** with a time-limited lock, then individually **acknowledged** as *accept*, *release* (redeliver), or *reject* (poison message).
- Each record carries a **delivery count**. After `group.share.delivery.count.limit` attempts it is archived instead of retried forever.
- The broker tracks, per partition, an in-flight window between the share-partition start offset and end offset. The **share coordinator** persists it in `__share_group_state`: again, a coordinator over a log.

| | Consumer group | Share group |
|---|---|---|
| Readers per partition | One | Many |
| Progress tracked as | One offset per partition | State per in-flight record |
| Ordering | Strict per partition | Not guaranteed |
| Good for | Event streams, CDC, aggregation | Work queues, jobs, fan-out workers |

## 12. Delivery guarantees

| Guarantee | How you get it | What failure looks like |
|---|---|---|
| **At most once** | Commit offsets *before* processing, or `acks=0`. | A crash loses the records between commit and processing. |
| **At least once** | `acks=all`, retries, commit *after* processing. | A crash reprocesses records since the last commit. Make processing idempotent. |
| **Exactly once** | Kafka-to-Kafka: transactions + `read_committed` + `sendOffsetsToTransaction`, or Kafka Streams with `processing.guarantee=exactly_once_v2`. | Output and offset commit succeed or abort together. |

> **Boundary:** exactly-once applies inside Kafka. Once you write to an external database or call an API, you need idempotent writes or your own deduplication on that side.

---

# Part 4: Control plane (KRaft)

## 13. Why KRaft exists

Every partition needs someone to decide who leads it, and every broker needs to know the answer. Until Kafka 3.x, that metadata lived in **ZooKeeper**, a separate distributed system, and one broker at a time acted as "the controller". Kafka 4.0 removed ZooKeeper completely. Metadata now lives in a Kafka-native Raft log. That is what **KRaft** (Kafka Raft) means.

### Before and after

```mermaid
flowchart TB
    subgraph Before["Before: ZooKeeper mode (removed in 4.0)"]
        ZK[("ZooKeeper ensemble<br/>separate system")]
        ZB1["B1 (controller)"]
        ZB2["B2"]
        ZB3["B3"]
        ZB1 <-- "znodes, watches" --> ZK
        ZB2 -. "register (ephemeral znode)" .-> ZK
        ZB3 -. "register (ephemeral znode)" .-> ZK
        ZB1 -- "push UpdateMetadata, LeaderAndIsr" --> ZB2
        ZB1 -- "push" --> ZB3
    end
    subgraph After["Now: KRaft mode"]
        subgraph KQ["Controller quorum"]
            K1["C1 active"]
            K2["C2 hot standby"]
            K3["C3 hot standby"]
        end
        KB1["B1"]
        KB2["B2"]
        KB3["B3"]
        KQ -- "records via Fetch (pull)" --> KB1
        KQ -- "pull" --> KB2
        KQ -- "pull" --> KB3
    end
```

Two things change: the external system disappears, and metadata delivery flips from **push to pull**.

- **In ZooKeeper mode**, a new controller had to read all metadata from ZooKeeper and then push it to every broker. On clusters with hundreds of thousands of partitions, that took minutes.
- **In KRaft**, standby controllers already replay the log, so failover is near-instant. Brokers catch up by reading from their last offset.

### What KRaft fixes

- One system to deploy, secure, and monitor.
- Controller failover in milliseconds to seconds, not minutes.
- Millions of partitions per cluster become practical.
- Metadata has an offset, so "how stale is this broker?" has a precise answer.

### What it reuses

- Kafka's own log format and segment files.
- Kafka's own `Fetch` RPC for replication.
- Snapshots, which play the role compaction plays for state.
- The same "state machine over a log" pattern as the coordinators.

## 14. The Raft quorum

The controllers are the **voters** of a Raft quorum that replicates one single-partition log, `__cluster_metadata`. Brokers are **observers**: they replicate the same log but do not vote.

You size the quorum for fault tolerance. Every decision needs a majority, so 3 voters survive 1 failure and 5 voters survive 2.

### Terms you need

- **Epoch**: Raft's "term". Each election starts a higher epoch. There is at most one leader per epoch.
- **Leader**: the only voter that appends to the log. In KRaft, the Raft leader *is* the active controller.
- **Committed / high watermark**: an entry is committed once a majority of voters has it. The controller acts on records only after commit.

### Election

1. A voter that hears nothing from a leader for `controller.quorum.fetch.timeout.ms` (2 s) starts an election.
2. **Pre-vote** (KIP-996): the voter first asks "would you vote for me?" without bumping the epoch. A voter that was merely partitioned away therefore cannot disrupt a healthy leader when it comes back.
3. If a majority would vote for it, it becomes a candidate, bumps the epoch, and asks for real votes.
4. A voter grants its vote only if the candidate's log is at least as up to date as its own: it compares last epoch first, then last offset.
5. Randomized timeouts (`controller.quorum.election.timeout.ms`, 1 s base) keep two candidates from colliding forever.

```mermaid
stateDiagram-v2
    direction LR
    [*] --> Unattached
    Unattached --> Prospective: fetch timeout, start pre-vote
    Prospective --> Candidate: majority would vote, bump epoch
    Prospective --> Follower: discovers current leader
    Candidate --> Leader: majority of votes
    Candidate --> Prospective: election timeout
    Candidate --> Follower: sees leader at same or higher epoch
    Unattached --> Follower: learns leader
    Follower --> Prospective: leader silent
    Leader --> Resigned: shutdown or loses quorum
    Resigned --> Unattached: new epoch
```

These are the real states, one class each, in `raft/src/main/java/org/apache/kafka/raft/`: `UnattachedState`, `ProspectiveState`, `CandidateState`, `LeaderState`, `FollowerState`, `ResignedState`. `QuorumState` owns the transitions and persists the epoch and vote to disk, so a restart cannot vote twice in the same epoch.

### Replication is pull, not push

Textbook Raft has the leader push `AppendEntries` to followers. KRaft inverts this to match Kafka:

- Followers and observers send `Fetch` requests with their next offset and the epoch of their last record.
- The fetch doubles as the heartbeat.
- If a follower's log has diverged (it holds records from an epoch that ended earlier on the leader), the leader replies with a **diverging epoch**, and the follower truncates and fetches again.

```mermaid
sequenceDiagram
    participant F as C2 (follower)
    participant L as C1 (leader, epoch 5)
    participant O as Broker (observer)
    F->>L: Fetch(offset=120, lastFetchedEpoch=4)
    L-->>F: divergingEpoch: epoch 4 ended at 118
    F->>F: truncate log to 118
    F->>L: Fetch(offset=118, lastFetchedEpoch=4)
    L-->>F: records 118..130 (epoch 5)
    F->>L: Fetch(offset=131)
    Note over L: majority (C1, C2) has 130<br/>high watermark → 131, records committed
    L-->>F: HW=131
    O->>L: Fetch(offset=97)
    L-->>O: records 97..130, HW=131
```

Each Fetch carries the follower's position, so the leader learns progress for free. The leader moves the high watermark when a majority of voters have fetched past an offset. Observers (brokers) never count toward that majority.

### Snapshots

The metadata log would grow forever, so controllers and brokers periodically write a **snapshot**: the full metadata state as of some offset. Snapshots live in `metadata.log.dir`, in files named `<offset>-<epoch>.checkpoint`. The log before a snapshot can then be deleted.

A brand-new broker, or one too far behind, downloads the latest snapshot with `FetchSnapshot` and then fetches only the records after it.

### Dynamic quorums

Since KIP-853 the voter set itself is stored in the log, so you can add or remove controllers at runtime (`kafka-metadata-quorum.sh add-controller` / `remove-controller`, using the `AddRaftVoter` / `RemoveRaftVoter` RPCs). New clusters bootstrap with `controller.quorum.bootstrap.servers` instead of a fixed `controller.quorum.voters` list.

## 15. The active controller

The Raft leader runs `QuorumController`. It is deliberately **single-threaded**: every request becomes an event on one queue, so there are no locks around metadata.

Handling an event works like this:

1. Validate the request against the current in-memory state.
2. Produce a list of metadata records, for example `TopicRecord`, `PartitionRecord`, `PartitionChangeRecord`, `RegisterBrokerRecord`, `FenceBrokerRecord`.
3. Apply them to in-memory state speculatively and append them to the Raft log.
4. Reply to the caller only once those records are **committed**. If the controller loses leadership first, it reverts to the last committed state and the request fails over.

The state is split into **managers**, each owning one kind of record:

| Manager | Owns |
|---|---|
| `ClusterControlManager` | Brokers and their registrations |
| `ReplicationControlManager` | Topics, partitions, leaders, ISR |
| `ConfigurationControlManager` | Dynamic configs |
| `FeatureControlManager` | `metadata.version` and other feature levels |
| `AclControlManager` | ACLs |

Standby controllers replay the same committed records into the same managers, so they are always ready to take over.

### Example: creating a topic

```mermaid
sequenceDiagram
    participant A as Admin client
    participant B as Any broker
    participant C as Active controller
    participant V as Other voters
    participant Bs as All brokers
    A->>B: CreateTopics(orders, partitions=3, rf=3)
    B->>C: forwarded CreateTopics
    C->>C: choose replicas per partition (rack-aware)
    C->>V: append TopicRecord + 3 PartitionRecords
    V-->>C: Fetch shows majority has them
    Note over C: records committed
    C-->>B: success
    B-->>A: success
    Bs->>C: Fetch __cluster_metadata
    C-->>Bs: TopicRecord, PartitionRecords
    Bs->>Bs: apply delta to MetadataImage
    Bs->>Bs: ReplicaManager creates logs, becomes leader or follower
```

Clients never talk to controllers directly; brokers forward admin requests. The topic exists from the moment its records are committed. Each broker starts serving it as soon as it fetches and applies those records.

## 16. Brokers in KRaft

### Registration and heartbeats

A broker joins the cluster by sending `BrokerRegistration` to the active controller, with its listeners, rack, supported features, and a unique incarnation id. It then sends `BrokerHeartbeat` every `broker.heartbeat.interval.ms` (2 s). Each heartbeat reports how far the broker has read the metadata log, and the controller's reply tells the broker whether it is fenced.

- A broker starts **fenced**. It is unfenced (eligible to lead partitions) only after it has caught up with the metadata log.
- If no heartbeat arrives within `broker.session.timeout.ms` (9 s), the controller writes a `FenceBrokerRecord` and moves leadership of that broker's partitions to other ISR members with `PartitionChangeRecord`s.
- On a clean shutdown, the broker asks for a **controlled shutdown**. The controller moves its leaderships away first, so clients see no errors.

### Broker lifecycle

```mermaid
stateDiagram-v2
    direction LR
    [*] --> STARTING
    STARTING --> RECOVERY: registered, catching up on metadata
    RECOVERY --> RUNNING: caught up, unfenced
    RUNNING --> PENDING_CONTROLLED_SHUTDOWN: shutdown requested
    PENDING_CONTROLLED_SHUTDOWN --> SHUTTING_DOWN: leaderships moved away
    SHUTTING_DOWN --> [*]
```

The states are defined in `metadata/src/main/java/org/apache/kafka/metadata/BrokerState.java`. On the broker side, `BrokerLifecycleManager` drives them.

### MetadataImage: how a broker sees the cluster

Each broker folds the metadata log into an immutable **`MetadataImage`** (topics, partitions, brokers, configs, ACLs, features).

- New records produce a `MetadataDelta`. Applying it yields a new image.
- Components such as `ReplicaManager` react to just what changed, for example "you are now leader of orders-1 at epoch 12".
- Client `Metadata` requests are answered from the current image, which is why every broker can answer them.

### Operations you will actually run

```bash
# format storage once per node (cluster id + initial controller set)
bin/kafka-storage.sh format -t $(bin/kafka-storage.sh random-uuid) \
  -c config/server.properties --standalone

# who is the leader, how far behind is everyone?
bin/kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --status
bin/kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --replication

# read the metadata log or a snapshot offline
bin/kafka-metadata-shell.sh --snapshot /var/lib/kafka/meta/__cluster_metadata-0/00000000000000000000.log
```

---

# Part 5: Putting it together

## 17. A record's whole journey

Follow one order event from `send()` to a processed result, with every component from this guide in the order it acts.

1. **Metadata.** On startup the producer fetches metadata from a bootstrap broker. That broker answers from its `MetadataImage`, built from `__cluster_metadata`, which the KRaft quorum committed.
2. **Partition.** `send(key="customer-42")` serializes the record, hashes the key to `orders-1`, and appends it to that partition's open batch in the accumulator.
3. **Batch.** After `linger.ms`, the Sender ships the batch, with producer ID and sequence number, to Broker 2, the leader of `orders-1`.
4. **Append.** Broker 2 validates the sequence, appends to the active segment, and parks the request until the batch is replicated (`acks=all`).
5. **Replicate.** Followers on Brokers 1 and 3 fetch the new records. Once both have them, the high watermark passes the batch and Broker 2 sends the ack. The Future completes.
6. **Find coordinator.** A consumer in group `billing` asks for its coordinator: `__consumer_offsets-9`, led by Broker 2.
7. **Join.** It heartbeats to the coordinator, which writes the new member and target assignment to `__consumer_offsets-9` and assigns it `orders-1`.
8. **Resume.** It fetches the committed offset for `orders-1` from the coordinator, then sends `Fetch` to Broker 2 starting there.
9. **Read.** Broker 2 returns batches below the high watermark, often straight from the page cache to the socket (zero-copy). `poll()` hands them to your code.
10. **Commit.** After processing, the consumer commits offset + 1. The coordinator appends an offset record to `__consumer_offsets-9` and replies once it is replicated.
11. **Failure.** If Broker 2 dies, the controller notices missed heartbeats after 9 s, fences it, and writes new leaders for `orders-1` and `__consumer_offsets-9`. Clients get `NotLeaderOrFollower`, refresh metadata, and continue against the new leaders. The new coordinator replays partition 9 to rebuild group state.

## 18. Configs that matter

| Config | Where | Default | What it controls |
|---|---|---|---|
| `acks` | producer | `all` | Durability vs latency of each write |
| `enable.idempotence` | producer | `true` | Dedup of retried batches |
| `linger.ms` / `batch.size` | producer | 5 ms / 16 KB | Batching, throughput |
| `buffer.memory` | producer | 32 MB | Accumulator size before `send()` blocks |
| `delivery.timeout.ms` | producer | 120 s | Total time to keep retrying a record |
| `transactional.id` | producer | none | Enables transactions and zombie fencing |
| `group.protocol` | consumer | `classic` | Old vs KIP-848 rebalance protocol |
| `enable.auto.commit` | consumer | `true` | Background offset commits every 5 s |
| `auto.offset.reset` | consumer | `latest` | Where to start with no committed offset |
| `max.poll.interval.ms` | consumer | 300 s | Max time between `poll()` calls before eviction |
| `isolation.level` | consumer | `read_uncommitted` | Whether to hide open/aborted transactions |
| `group.instance.id` | consumer | none | Static membership |
| `replication.factor` | topic | 1 (set 3) | Copies per partition |
| `min.insync.replicas` | topic / broker | 1 (set 2) | Minimum ISR for `acks=all` writes |
| `retention.ms` | topic | 7 days | How long data is kept |
| `cleanup.policy` | topic | `delete` | Delete old segments or compact by key |
| `unclean.leader.election.enable` | topic / broker | `false` | Allow electing an out-of-sync leader |
| `replica.lag.time.max.ms` | broker | 30 s | When a slow follower leaves the ISR |
| `offsets.topic.num.partitions` | broker | 50 | Spread of group coordinators |
| `group.consumer.session.timeout.ms` | broker | 45 s | KIP-848 member liveness |
| `group.consumer.heartbeat.interval.ms` | broker | 5 s | KIP-848 heartbeat interval |
| `process.roles` | node | none | `broker`, `controller`, or both |
| `broker.heartbeat.interval.ms` | broker | 2 s | Broker → controller heartbeat |
| `broker.session.timeout.ms` | controller | 9 s | When a silent broker is fenced |
| `controller.quorum.fetch.timeout.ms` | controller | 2 s | Leader silence before an election |
| `controller.quorum.election.timeout.ms` | controller | 1 s | Base election timeout (randomized) |

## 19. Where it lives in the code

Paths are relative to the root of the Kafka source tree.

| Concept | Start reading at |
|---|---|
| Producer batching | `clients/src/main/java/org/apache/kafka/clients/producer/internals/RecordAccumulator.java`, `Sender.java`, `BuiltInPartitioner.java` |
| Producer transactions | `clients/.../producer/internals/TransactionManager.java` |
| Consumers | `clients/.../consumer/internals/AsyncKafkaConsumer.java` (new), `ClassicKafkaConsumer.java` |
| Partition log on disk | `storage/src/main/java/org/apache/kafka/storage/internals/log/UnifiedLog.java`, `LogSegment.java`, `ProducerStateManager.java` |
| Leader epochs | `storage/src/main/java/org/apache/kafka/storage/internals/epoch/LeaderEpochFileCache.java` |
| Replication, ISR, HW | `core/src/main/scala/kafka/cluster/Partition.scala`, `core/src/main/scala/kafka/server/ReplicaManager.scala` |
| Group coordinator | `group-coordinator/src/main/java/org/apache/kafka/coordinator/group/GroupCoordinatorService.java`, `GroupMetadataManager.java`, `modern/consumer/ConsumerGroup.java` |
| Server-side assignors | `group-coordinator/src/main/java/org/apache/kafka/coordinator/group/assignor/` |
| Assignor plugin API | `group-coordinator/group-coordinator-api/src/main/java/org/apache/kafka/coordinator/group/api/assignor/ConsumerGroupPartitionAssignor.java` |
| Coordinator engine | `coordinator-common/src/main/java/org/apache/kafka/coordinator/common/runtime/CoordinatorRuntime.java` |
| Share groups | `group-coordinator/.../modern/share/ShareGroup.java`, `share-coordinator/.../ShareCoordinatorService.java` |
| Transaction coordinator | `core/src/main/scala/kafka/coordinator/transaction/TransactionCoordinator.scala` |
| Raft | `raft/src/main/java/org/apache/kafka/raft/KafkaRaftClient.java`, `QuorumState.java`, `LeaderState.java` |
| Active controller | `metadata/src/main/java/org/apache/kafka/controller/QuorumController.java`, `ReplicationControlManager.java`, `ClusterControlManager.java` |
| Broker metadata view | `metadata/src/main/java/org/apache/kafka/image/MetadataImage.java`, `server/src/main/java/org/apache/kafka/server/BrokerLifecycleManager.java` |
| Broker states | `metadata/src/main/java/org/apache/kafka/metadata/BrokerState.java` |
| Wire protocol | `clients/src/main/resources/common/message/*.json`, e.g. `ProduceRequest.json`, `FetchRequest.json`, `ConsumerGroupHeartbeatRequest.json`, `VoteRequest.json` |

**A good reading order:** read the message JSON files first, because they show exactly what each side sends. Then read the client class that sends the request, then the broker class that handles it.

---

# Part 6: Under the hood

Parts 1–5 explain what Kafka does. Part 6 explains how it does it fast. This is the level where you can reason about latency numbers and disk usage instead of guessing.

## 20. The record batch format

Kafka never stores or sends single records. The unit on disk, on the wire, and in the producer's buffer is the **record batch**. The broker writes the batch the producer sent almost byte for byte, and a consumer receives the same bytes. That is why the broker can serve reads without decoding them.

```
RecordBatch (61-byte header, then the records)
┌──────────────────────┬────────┬─────────────────────────────────────────────────┐
│ BaseOffset           │ int64  │ offset of the first record (set by the leader)  │
│ Length               │ int32  │ size of the rest of the batch                   │
│ PartitionLeaderEpoch │ int32  │ leader epoch when it was appended               │
│ Magic                │ int8   │ format version, 2                               │
│ CRC                  │ uint32 │ checksum of everything after this field         │
│ Attributes           │ int16  │ compression, timestamp type, transactional,     │
│                      │        │ control flag                                    │
│ LastOffsetDelta      │ int32  │ last record's offset minus BaseOffset           │
│ BaseTimestamp        │ int64  │ first record's timestamp                        │
│ MaxTimestamp         │ int64  │ largest timestamp in the batch                  │
│ ProducerId           │ int64  │ ┐                                               │
│ ProducerEpoch        │ int16  │ ├ idempotence and transactions                  │
│ BaseSequence         │ int32  │ ┘                                               │
│ RecordsCount         │ int32  │ number of records                               │
│ Records              │ ...    │ compressed together as one block                │
└──────────────────────┴────────┴─────────────────────────────────────────────────┘

Record (each one inside Records)
┌──────────────────────┬────────────────┬──────────────────────────────────────────┐
│ Length               │ varint         │ size of this record                      │
│ Attributes           │ int8           │ unused                                   │
│ TimestampDelta       │ varlong        │ timestamp minus BaseTimestamp            │
│ OffsetDelta          │ varint         │ offset minus BaseOffset                  │
│ Key                  │ varint + bytes │ length, then bytes (-1 = null)           │
│ Value                │ varint + bytes │ length, then bytes (-1 = null)           │
│ Headers              │ varint + list  │ count, then key/value pairs              │
└──────────────────────┴────────────────┴──────────────────────────────────────────┘
```

Why this layout matters:

- **Per-batch, not per-record, overhead.** Producer id, epoch, sequence, timestamps, and the CRC are stored once per batch. Each record stores only small varint *deltas*. Bigger batches therefore mean less overhead per record.
- **Compression covers the whole batch.** Similar records compress well together. A batch of one record barely compresses at all.
- **Offsets are assigned by the broker.** The producer sends the batch with `BaseOffset = 0`. The leader overwrites `BaseOffset` with the next offset in the log and recomputes nothing else, because every record stores only an offset delta.
- **Control records** (transaction commit and abort markers) are batches with the control flag set. Consumers skip them, but they still use an offset. That is why offsets in a transactional topic have gaps.
- **Headers** belong to each record. Use them for metadata such as trace ids, schema ids, or the dead-letter information this project's `EventConsumer` adds.

The format is defined in `clients/src/main/java/org/apache/kafka/common/record/internal/DefaultRecordBatch.java` and `DefaultRecord.java`. To see real batches on disk:

```bash
bin/kafka-dump-log.sh --files /var/lib/kafka/data/orders-0/00000000000000000000.log \
  --deep-iteration --print-data-log
```

## 21. Inside a broker: how a request is processed

Every request, whether Produce, Fetch, or Heartbeat, goes through the same pipeline. Knowing it lets you read the broker's latency metrics and say exactly where time is spent.

```mermaid
flowchart LR
    C["Client"] -- "TCP" --> A["Acceptor thread<br/>(one per listener)"]
    A --> N["Network threads<br/>num.network.threads = 3<br/>read and write sockets"]
    N -- "enqueue" --> RQ[("Request queue<br/>queued.max.requests = 500")]
    RQ --> H["I/O (handler) threads<br/>num.io.threads = 8<br/>KafkaApis: append, read, ..."]
    H -- "must wait for something" --> P["Purgatory<br/>DelayedProduce, DelayedFetch"]
    P -- "condition met or timeout" --> RS
    H -- "done now" --> RS[("Response queue<br/>(one per network thread)")]
    RS --> N
    N -- "TCP" --> C
```

- **Network threads** only move bytes between sockets and queues. They never touch the log.
- **I/O threads** (request handlers) do the real work in `KafkaApis`: appending to the log, reading from it, coordinator logic.
- **Purgatory** holds requests that cannot finish yet, without blocking a thread:
  - **DelayedProduce:** an `acks=all` produce waiting for followers to replicate the batch.
  - **DelayedFetch:** a consumer fetch waiting until `fetch.min.bytes` of data are available or `fetch.max.wait.ms` passes. This is Kafka's long polling.
  - Delayed requests are tracked with a hierarchical timing wheel, so millions of pending timeouts are cheap.

### Reading the latency breakdown

For every request type, the broker publishes `kafka.network:type=RequestMetrics,name=<metric>,request=<Produce|Fetch|...>`:

```
TotalTimeMs = RequestQueueTimeMs + LocalTimeMs + RemoteTimeMs + ThrottleTimeMs + ResponseQueueTimeMs + ResponseSendTimeMs
```

| Component | Time spent | If it is high |
|---|---|---|
| `RequestQueueTimeMs` | Waiting for a free I/O thread | I/O threads are saturated. Check `RequestHandlerAvgIdlePercent`. |
| `LocalTimeMs` | Leader doing the work (append or read) | Slow disk, page cache misses, lock contention |
| `RemoteTimeMs` | In purgatory | Produce: followers are slow to replicate. Fetch: normal long-poll waiting. |
| `ThrottleTimeMs` | Quota throttling | A client exceeds its quota |
| `ResponseQueueTimeMs` | Waiting for a network thread | Network threads are saturated. Check `NetworkProcessorAvgIdlePercent`. |
| `ResponseSendTimeMs` | Writing the response to the socket | Slow client or network |

For example: produce p99 is 200 ms, and almost all of it is `RemoteTimeMs`. The leader is waiting for followers, so look at replica fetchers and follower disks, not at the leader.

## 22. The storage engine: page cache, zero-copy, and durability

### Kafka relies on the OS page cache

Kafka does not keep its own cache of records. Writes go to the file system, which places them in the OS **page cache** (RAM) and writes them to disk in the background. Reads of recent data are served from the same page cache.

```
   producer ──► broker JVM ──write()──► page cache (RAM) ──later──► disk
                                              │
   consumer ◄── socket ◄──sendfile()──────────┘   (tail reads never touch the disk)
```

Consequences:

- **Give Kafka a small JVM heap and a lot of free RAM.** A broker typically runs with a 6 GB heap on a machine with 64 GB or more. The rest becomes page cache.
- **Consumers that keep up are almost free.** They read what was just written, straight from memory.
- **Lagging consumers are expensive.** Reading old data pulls it from disk and can push the hot tail out of the page cache, which slows every other consumer. Tiered storage and separate clusters for replay-heavy workloads are the usual fixes.

### Zero-copy

For plaintext connections, the broker sends log data to a consumer with `sendfile()`, a system call that copies directly from the page cache to the socket without passing through the JVM. This is why the broker does not decode or recompress batches on reads. **TLS disables zero-copy**, because the data must be encrypted in user space, so TLS costs more CPU on the broker.

### Durability comes from replication, not fsync

By default Kafka **does not call fsync** on each write (`flush.messages` and `flush.ms` default to "never"; the OS flushes when it chooses). A batch acknowledged with `acks=all` is in the page cache of every in-sync replica, not necessarily on any disk.

Kafka accepts this because losing data would require every ISR replica to lose power at the same moment, before the OS flushes. Spreading replicas across racks or availability zones (`broker.rack`) makes that very unlikely. Forcing fsync per message costs a lot of throughput for little extra safety.

### Recovery after an unclean shutdown

If a broker crashes, the tail of its active segment may be incomplete. On restart, the broker scans segments written after the last recovery checkpoint, validates each batch's CRC, rebuilds indexes, and truncates anything corrupt. Recovery runs `num.recovery.threads.per.data.dir` (2) threads per log directory. On brokers with many partitions this can take minutes, so raise that value. A clean shutdown writes a marker file that skips this scan.

### Indexes and timestamps

- The offset index is **sparse**: one entry every `index.interval.bytes` (4096 bytes) of log. Finding an offset is a binary search in the index followed by a short scan of the segment.
- Each record has a timestamp. `message.timestamp.type=CreateTime` (the default) keeps the producer's timestamp. `LogAppendTime` makes the broker overwrite it with its own clock, which is safer for time-based retention when producer clocks cannot be trusted.
- Time-based retention deletes a segment when its **largest** timestamp is older than `retention.ms`. One record with a timestamp far in the future can keep a whole segment alive. `log.message.timestamp.after.max.ms` rejects such records.

---

# Part 7: Designing with Kafka

## 23. Topic and partition design

### How many partitions?

Choose the partition count for the highest throughput and largest consumer group you expect, because changing it later moves keys to different partitions.

```
partitions = max( target throughput / throughput one producer can push into a partition,
                  target throughput / throughput one consumer can process from a partition )
```

Example: you need 200 MB/s. One partition takes about 10 MB/s from producers, but one consumer instance processes only 5 MB/s.

```
max(200 / 10, 200 / 5) = max(20, 40) = 40 partitions, then add room for growth → 48 or 60
```

Measure both rates for your own records with `kafka-producer-perf-test.sh` and `kafka-consumer-perf-test.sh`. Numbers vary enormously with record size and processing cost.

### Why not 10,000 partitions?

| Cost | Why |
|---|---|
| Producer memory and batching | Batches are per partition. Spread over many partitions, each batch is smaller, so compression and throughput drop. |
| Open files and memory on brokers | Every partition replica has segment, index, and time-index files. |
| End-to-end latency | A follower replicates many partitions through a few fetcher threads (`num.replica.fetchers`, default 1). |
| Rebalances and failover | More partitions to move and more leaders to elect when a broker fails. |

KRaft raised the practical limit a lot, but a few thousand partitions per broker is still a sensible ceiling for most hardware.

### Sizing disk and network

```
total disk   = ingress × retention × replication factor ÷ 0.7     (keep 30% free)
broker in    = (ingress + ingress × (RF − 1)) ÷ brokers           (producers + replication)
broker out   = (ingress × consumer groups + ingress × (RF − 1)) ÷ brokers
```

Example: 50 MB/s ingress, 7 days retention, RF 3, 3 consumer groups, 6 brokers.

```
total disk = 50 MB/s × 604,800 s × 3 ÷ 0.7 ≈ 130 TB  →  ~21.6 TB per broker
broker in  = (50 + 100) ÷ 6 = 25 MB/s
broker out = (150 + 100) ÷ 6 ≈ 42 MB/s
```

Each consumer group reads the data once more, so egress grows with every new group. Fetch-from-follower (`client.rack`) does not reduce total egress, but it keeps it inside one availability zone, which matters for cloud bills.

### Topic conventions

- **Name topics by domain and event**, for example `payments.card-authorized.v1`. Avoid names that describe the consumer, because topics outlive the services that read them.
- **One event type per topic** is simplest. Several types in one topic is right when you need their *relative order*, for example all events of one account, which must share a key and a partition.
- **Disable automatic topic creation** in production (`auto.create.topics.enable=false`) and create topics on purpose, with explicit partitions, RF, and retention.
- **Always set `replication.factor=3` and `min.insync.replicas=2`** for data you cannot lose.

## 24. Keys, schemas, and message size

### Choosing keys

The key decides three things: the partition, the order, and which records compaction keeps.

| Goal | Key |
|---|---|
| Order all events of one entity | The entity id (`order-id`, `account-id`) |
| Join two topics | The same key, with the same partition count and partitioner in both |
| A compacted "latest value" table | The entity id. The value is the full current state. |
| Spread load evenly, no ordering needed | `null` (the sticky partitioner spreads batches) |

A key with very uneven traffic creates a **hot partition** that only one consumer can read. If one customer is 30% of traffic, that partition limits the whole group.

### Schemas and evolution

Kafka stores bytes. It does not know or check your format. Producers and consumers must agree on it, and that agreement changes over time.

- **Use a schema format with evolution rules:** Avro, Protobuf, or JSON Schema. Plain JSON without a schema works until someone renames a field.
- A **schema registry** (Confluent Schema Registry, Apicurio, Karapace) stores schemas and gives each one an id. The producer puts the id in the first bytes of the value; the consumer looks the schema up.
- **Compatibility modes** decide which changes are allowed:

| Mode | Allowed change | Upgrade order |
|---|---|---|
| Backward | New schema can read old data (e.g. add an optional field, delete a field) | Upgrade consumers first |
| Forward | Old schema can read new data (e.g. add a field, delete an optional field) | Upgrade producers first |
| Full | Both | Either order |

- For a change that breaks compatibility, create a **new topic** (`...v2`), write to both for a while, and move consumers over.

### Message size

| Setting | Default | Where |
|---|---|---|
| `message.max.bytes` (`max.message.bytes` per topic) | ~1 MB | Broker / topic: largest batch accepted |
| `max.request.size` | 1 MB | Producer: largest request |
| `max.partition.fetch.bytes` | 1 MB | Consumer: per partition per fetch (a larger batch is still returned so the consumer is never stuck) |
| `fetch.max.bytes` | 50 MB | Consumer: per fetch response |

Kafka works best with records from a few hundred bytes to a few hundred KB. For large payloads (files, images), use the **claim check** pattern: store the payload in object storage and send a small record with its location.

## 25. Reliability patterns

### Idempotent consumers

At-least-once delivery means **any record can be processed twice**: after a crash, a rebalance, or a failed commit. Design processing so a second run does no harm:

- Upsert by a natural key instead of inserting.
- Store the processed `(topic, partition, offset)` or an event id together with the result, in the same database transaction, and skip duplicates.
- Make external calls idempotent (for example, with idempotency keys on payment APIs).

### The transactional outbox

A service often needs to update its database *and* publish an event. Doing both directly cannot be atomic: the database commit can succeed while the produce fails, or the other way round.

```mermaid
flowchart LR
    S["Service"] -- "one DB transaction:<br/>update orders + insert into outbox" --> DB[("Database")]
    DB -- "change data capture<br/>(e.g. Debezium on Kafka Connect)" --> K["Kafka topic"]
    K --> C["Consumers"]
```

The service writes the event to an `outbox` table in the same transaction as the business change. A CDC connector reads the database log and publishes the outbox rows. Either both happen or neither does.

### Errors: retry, dead-letter, or stop

When processing a record fails, you have three options:

```mermaid
flowchart TD
    R["Record fails"] --> T{"Transient?<br/>(timeout, 503, lock)"}
    T -- yes --> Retry["Retry in place with backoff<br/>(blocks this partition meanwhile)"]
    Retry -- "still failing after N tries" --> D
    T -- "no (bad data, bug)" --> D{"Dead-letter topic configured?"}
    D -- yes --> DLQ["Send original bytes + error headers<br/>to the DLT, commit, continue"]
    D -- no --> Stop["Stop the consumer<br/>(never skip silently)"]
```

- **Poison pills** are records that can never be processed, for example because they cannot be deserialized. Without a dead-letter topic they block their partition forever.
- **Keep the original bytes** in the dead-letter record, plus headers with the source topic, partition, offset, and error. Then records can be inspected and replayed.
- **Retrying in place blocks the partition**, which preserves order but stops progress. **Retry topics** (`orders.retry.1m`, `orders.retry.10m`) let the main partition continue, but they give up ordering for retried records.

This project's `EventConsumer` implements the first two branches: per-record retries with `RetryPolicy`, then a dead-letter topic with `dlq.*` headers, or stopping when no dead-letter topic is configured.

## 26. Kafka Streams

Kafka Streams is a Java library (not a cluster) for processing data from Kafka topics into Kafka topics. Your application *is* the stream processor; scaling it means running more instances with the same `application.id`.

```mermaid
flowchart LR
    In["orders topic"] --> F["filter / map<br/>(stateless)"]
    F --> G["groupByKey + count<br/>(stateful)"]
    G <--> SS[("state store<br/>RocksDB on local disk")]
    SS -. "every update" .-> CL["changelog topic<br/>(compacted)"]
    G --> Out["order-counts topic"]
```

Core ideas:

| Concept | Meaning |
|---|---|
| `KStream` | An event stream: every record is a new fact |
| `KTable` | A changelog: each record is the latest value for its key |
| `GlobalKTable` | A table fully copied to every instance (for small lookup data) |
| Task | The unit of parallelism: one task per input partition (per sub-topology) |
| State store | Local RocksDB (or in-memory) store for aggregations and joins |
| Changelog topic | Compacted topic backing each store, used to rebuild it after a failure or move |
| Repartition topic | Internal topic created when you change the key before a stateful step |
| Standby replica | A warm copy of a store on another instance (`num.standby.replicas`) for fast failover |

Things to know:

- **Joins need co-partitioning:** both inputs must have the same key and the same number of partitions. Otherwise Streams has to repartition, which costs an extra topic and network round trip.
- **Windows:** tumbling (fixed, non-overlapping), hopping (fixed, overlapping), sliding, and session (gaps of inactivity). A **grace period** decides how long late records are still accepted.
- **Exactly-once:** `processing.guarantee=exactly_once_v2` wraps "read, update state, write output, commit offsets" in one Kafka transaction.
- **Rebalancing:** Streams groups (KIP-1071, `kafka-streams-groups.sh`) move task assignment to the broker, like KIP-848 does for consumers.
- **Internal topics** are named `<application.id>-<name>-changelog` and `-repartition`. Reset an application with `kafka-streams-application-reset.sh`.

## 27. Kafka Connect

Kafka Connect moves data between Kafka and other systems (databases, S3, Elasticsearch...) without writing code. You configure **connectors**; Connect runs them.

```mermaid
flowchart LR
    DB[("PostgreSQL")] --> SC
    subgraph Workers["Connect cluster (distributed mode)"]
        SC["Source connector<br/>tasks 1..N"] --> CV1["converter<br/>(Avro, JSON, ...)"]
        SMT1["SMTs<br/>single message transforms"]
        CV1 --> SMT1
        SK["Sink connector<br/>tasks 1..N"]
    end
    SMT1 --> K["Kafka topics"]
    K --> SK
    SK --> S3[("S3 / warehouse")]
    Workers <-. "config, offsets, status" .-> IT[("internal topics<br/>connect-configs, connect-offsets, connect-status")]
```

| Concept | Meaning |
|---|---|
| Worker | A JVM running Connect. Workers with the same `group.id` form a cluster and share the work. |
| Connector | A configured integration (e.g. "this database into these topics"). Splits work into tasks. |
| Task | The unit that actually copies data. Tasks are spread across workers. |
| Converter | Turns Connect's internal data into bytes and back (`key.converter`, `value.converter`). |
| SMT | A small per-record change: rename a field, route to another topic, add a timestamp. |
| Internal topics | `config.storage.topic`, `offset.storage.topic`, `status.storage.topic`. Connect's own state, stored in Kafka. |

- Manage connectors through the REST API (port 8083 by default): `POST /connectors`, `GET /connectors/<name>/status`, `PUT /connectors/<name>/pause`.
- **Error handling for sinks:** `errors.tolerance=all` with `errors.deadletterqueue.topic.name` sends bad records to a dead-letter topic instead of failing the task.
- **Exactly-once source connectors** (KIP-618) are available when the connector supports them (`exactly.once.source.support=enabled` on workers).
- Connect has its own REST security model; see `docs/security/security-model-connect.md`.

## 28. Multiple clusters and disaster recovery

### Options

| Approach | How | Data loss on regional failure (RPO) | Notes |
|---|---|---|---|
| Stretch cluster | One cluster across 3 availability zones, `broker.rack` set, RF 3, `min.insync.replicas=2` | 0 | Synchronous. Needs low latency between zones. Survives one zone, not one region. |
| MirrorMaker 2, active/passive | Replicate topics from primary to a standby cluster | Seconds (asynchronous) | Fail over by moving clients to the standby. |
| MirrorMaker 2, active/active | Each region produces locally, and topics are mirrored both ways | Seconds | Consumers read local + mirrored topics. Needs care with duplicates and ordering. |

### MirrorMaker 2

MirrorMaker 2 runs on Kafka Connect with three connectors:

- **MirrorSourceConnector** copies topic data and topic configs.
- **MirrorCheckpointConnector** translates consumer group offsets, so a group can continue in the other cluster near where it stopped.
- **MirrorHeartbeatConnector** emits heartbeats to measure replication health and latency.

By default, remote topics get the source cluster's alias as a prefix: `orders` from cluster `primary` becomes `primary.orders` (`DefaultReplicationPolicy`). This prevents replication loops in active/active setups. `IdentityReplicationPolicy` keeps the same name for simple active/passive setups.

Offsets differ between clusters (the same record has a different offset in each). That is why offset translation through checkpoints is needed. Plan for some reprocessing after a failover.

---

# Part 8: Running Kafka

## 29. Performance tuning

### Producer

| Goal | Settings |
|---|---|
| Throughput | `linger.ms` 10–100, `batch.size` 64–256 KB, `compression.type=lz4` or `zstd`, enough `buffer.memory` |
| Latency | `linger.ms` 0–5, smaller batches, `acks=1` only if you accept the loss risk |
| Durability | `acks=all`, `enable.idempotence=true`, topic `min.insync.replicas=2` |

Watch `record-error-rate`, `request-latency-avg`, `batch-size-avg`, `records-per-request-avg`, and `buffer-available-bytes` in the producer's metrics. This project's `ProducerOptions` defaults to `linger.ms=10`, 64 KB batches, and LZ4, a throughput-friendly starting point.

### Consumer

| Goal | Settings |
|---|---|
| Throughput | Raise `fetch.min.bytes` (e.g. 64 KB–1 MB) with `fetch.max.wait.ms` 100–500, `max.partition.fetch.bytes` if records are large |
| Latency | `fetch.min.bytes=1` (the default) |
| Slow processing | Lower `max.poll.records`, or process in parallel per partition |

Watch `records-lag-max`, `time-between-poll-avg`, `last-poll-seconds-ago`, `commit-latency-avg`, and `rebalance-latency-avg`.

### Broker and OS

- **Threads:** raise `num.io.threads` if `RequestHandlerAvgIdlePercent` drops below about 30%, and `num.network.threads` if `NetworkProcessorAvgIdlePercent` does. Raise `num.replica.fetchers` if followers lag.
- **JVM:** G1 GC with a 6–8 GB heap. A bigger heap does not help; the page cache does.
- **Linux:** XFS or ext4 with `noatime`, `vm.swappiness=1`, a high file-descriptor limit (100,000+), and a high `vm.max_map_count` (each segment index is memory-mapped).
- **Disks:** several disks in `log.dirs` (JBOD) or RAID 10. Keep the KRaft metadata log on fast storage.
- **Measure, then change one thing at a time** with `kafka-producer-perf-test.sh`, `kafka-consumer-perf-test.sh`, and `kafka-e2e-latency.sh`.

## 30. Operations runbook

### Upgrading a cluster

```mermaid
flowchart LR
    A["Read the upgrade notes<br/>docs/getting-started/upgrade.md"] --> B["Rolling restart:<br/>controllers first, then brokers,<br/>on the new binaries"]
    B --> C["Verify health<br/>(no under-replicated partitions)"]
    C --> D["Finalize the new features:<br/>kafka-features.sh upgrade --release-version X"]
```

Until the last step, the cluster keeps the old `metadata.version`, so you can still roll back the binaries. After finalizing, new features are enabled and downgrade may not be possible.

```bash
bin/kafka-features.sh --bootstrap-server localhost:9092 describe
bin/kafka-features.sh --bootstrap-server localhost:9092 upgrade --release-version 4.4 --dry-run
```

Restart one node at a time, and wait until `UnderReplicatedPartitions` is 0 before the next.

### Moving partitions

**Adding a broker does not move any data to it.** Only new partitions land there. To use it, reassign existing partitions:

```bash
# 1. propose a plan for some topics across brokers 1-4
bin/kafka-reassign-partitions.sh --bootstrap-server localhost:9092 \
  --topics-to-move-json-file topics.json --broker-list 1,2,3,4 --generate

# 2. run it, throttled so replication does not starve clients (bytes/s)
bin/kafka-reassign-partitions.sh --bootstrap-server localhost:9092 \
  --reassignment-json-file plan.json --execute --throttle 50000000

# 3. check progress; --verify also removes the throttle when done
bin/kafka-reassign-partitions.sh --bootstrap-server localhost:9092 \
  --reassignment-json-file plan.json --verify
```

For large clusters, tools such as Cruise Control generate and run balanced plans automatically.

### Removing a broker

1. Reassign all its partitions to other brokers.
2. Shut it down cleanly.
3. Remove its registration: `bin/kafka-cluster.sh unregister --bootstrap-server ... --id <broker-id>`.

### Leadership balance

The first replica in each partition's list is the **preferred leader**. After restarts, leadership can pile up on a few brokers. With `auto.leader.rebalance.enable=true` (the default) the controller moves it back periodically. To do it by hand:

```bash
bin/kafka-leader-election.sh --bootstrap-server localhost:9092 --election-type preferred --all-topic-partitions
```

### Other useful tools

| Tool | Use |
|---|---|
| `kafka-log-dirs.sh --describe` | Disk usage per partition and log directory |
| `kafka-get-offsets.sh` | Earliest and latest offsets per partition, or offsets for a timestamp |
| `kafka-delete-records.sh` | Delete everything before an offset (moves the log start offset) |
| `kafka-transactions.sh` | List transactions, find and abort hanging ones |
| `kafka-dump-log.sh` | Decode segment, index, or metadata log files |
| `kafka-metadata-quorum.sh` | KRaft leader and replication lag |
| `kafka-configs.sh` | Read and change dynamic configs for topics, brokers, users, clients |

## 31. Monitoring: the metrics that matter

### Cluster health (alert on these)

| Metric | Healthy value | Meaning when not |
|---|---|---|
| `kafka.controller:ActiveControllerCount` | Sum across controllers = 1 | 0: no active controller. More than 1: serious problem. |
| `kafka.controller:OfflinePartitionsCount` | 0 | Partitions with no leader: no reads or writes possible |
| `kafka.server:UnderReplicatedPartitions` | 0 | Followers behind: a broker is slow or down |
| `kafka.server:UnderMinIsrPartitionCount` | 0 | `acks=all` writes to these partitions are failing |
| `kafka.server:AtMinIsrPartitionCount` | 0 | One more failure and writes start failing |
| `kafka.controller:FencedBrokerCount` | 0 | A broker stopped heartbeating |
| `kafka.controller:UncleanLeaderElectionsPerSec` | 0 | Data was probably lost |

### Capacity and performance

| Metric | What to watch |
|---|---|
| `RequestHandlerAvgIdlePercent`, `NetworkProcessorAvgIdlePercent` | Below ~30%: the broker is running out of threads |
| `RequestMetrics` (`TotalTimeMs` and its parts, section 21) | Produce and fetch latency, and where the time goes |
| `BytesInPerSec`, `BytesOutPerSec`, `MessagesInPerSec` | Load per broker and per topic |
| `IsrShrinksPerSec`, `IsrExpandsPerSec` | Frequent flapping means overloaded brokers or network problems |
| `PartitionCount`, `LeaderCount` per broker | Imbalance |
| Disk usage per log directory | Alert well before full: a full disk takes the broker offline |
| Consumer lag (`records-lag-max` or `kafka-consumer-groups.sh`) | The most important application-level signal |

### KRaft

| Metric | Meaning |
|---|---|
| `current-state` (raft metrics) | Leader, follower, candidate... for each controller |
| `LastAppliedRecordOffset`, `LastAppliedRecordLagMs` | How far behind each node's metadata is |
| `EventQueueTimeMs` | How long requests wait in the controller's single-threaded queue |
| `ActiveBrokerCount`, `FencedBrokerCount` | Broker registrations |

## 32. Troubleshooting playbook

| Symptom | Likely causes | What to check and do |
|---|---|---|
| Consumer lag keeps growing | Processing too slow, too few partitions or consumers, hot partition, rebalance loop | Per-partition lag (`--describe`): one partition or all? Processing time per record. `rebalance-latency` and rebalance count. |
| Constant rebalances | `max.poll.interval.ms` exceeded, session timeouts from GC pauses, frequent deploys | Consumer logs for "leaving group", `time-between-poll-avg`. Use static membership, smaller `max.poll.records`, the new consumer protocol. |
| `NotEnoughReplicasException` on produce | ISR smaller than `min.insync.replicas` | `UnderMinIsrPartitionCount`. Which follower is out of sync and why (disk, network, GC)? |
| `TimeoutException` / `delivery.timeout.ms` expired | Broker unreachable, leader unavailable, ISR too small, producer buffer full | Producer `record-error-rate`, broker availability, `OfflinePartitionsCount` |
| Produce latency spikes | Follower replication slow (`RemoteTimeMs`), disk saturation (`LocalTimeMs`), thread exhaustion (`RequestQueueTimeMs`) | Request latency breakdown (section 21) |
| `RecordTooLargeException` | Batch larger than `max.request.size` or `max.message.bytes` | Raise limits consistently, or use the claim check pattern |
| `CommitFailedException` | Member was removed from the group before committing | Processing exceeded `max.poll.interval.ms`. Commit more often or process faster. |
| Consumer stuck at one offset | Poison pill, deserialization error | Logs. Add a dead-letter topic or skip with `kafka-consumer-groups.sh --reset-offsets --shift-by 1` (deliberately). |
| `read_committed` consumers stuck | A hanging (never-finished) transaction blocks the last stable offset | `kafka-transactions.sh find-hanging`, then `abort` |
| Broker slow to start | Log recovery after an unclean shutdown | Logs show recovery progress. Raise `num.recovery.threads.per.data.dir`. |
| Disk full | Retention too long for the traffic, uneven partition placement, tiered storage uploads failing. (Never a slow consumer: Kafka deletes by retention, not by consumption.) | `kafka-log-dirs.sh`, retention configs, `RemoteCopyErrorsPerSec` |

## 33. Testing Kafka applications

| Level | Tool | Good for |
|---|---|---|
| Unit | `MockProducer`, `MockConsumer` from `kafka-clients` | Logic around the client without a broker |
| Unit (Streams) | `TopologyTestDriver` from `kafka-streams-test-utils` | Kafka Streams topologies, with simulated time |
| Integration | In-JVM KRaft cluster (`kafka-test-common-runtime`) | Real broker behaviour, no Docker |
| Integration | Testcontainers (`apache/kafka` image) | Real broker in a container, closer to production |
| System | `kafka-verifiable-producer.sh` / `-consumer.sh`, Trogdor | Fault injection and load, as Kafka's own system tests do |

This project uses the in-JVM KRaft cluster (see `project/Dependencies.scala` and `src/test/scala/kafkainpractice/client/KafkaClientsIntegrationSpec.scala`), with `Test / fork := true` so the broker gets its own JVM.

Test the failure cases, not only the happy path:

- A record that cannot be deserialized: does it go to the dead-letter topic?
- A handler that fails a few times and then succeeds: is it retried, and processed only once?
- A consumer stopped in the middle of a batch: does the next one continue from the last commit without losing records?
- Two consumers in one group: do they split the partitions, and does one take over when the other stops?

## 34. Anti-patterns

| Anti-pattern | Why it hurts | Instead |
|---|---|---|
| Using Kafka as a database with lookups by key | Kafka has no index by key. Finding a record means scanning. | Materialize a compacted topic into a store (Kafka Streams, a database) |
| One topic per user or per entity | Millions of partitions | One topic, keyed by user |
| `acks=1` or RF 2 for important data | One failure can lose acknowledged data | `acks=all`, RF 3, `min.insync.replicas=2` |
| Huge messages (10 MB+) | Memory pressure, slow replication, fetch problems | Claim check pattern |
| Synchronous `send().get()` per record | One round trip per record, very low throughput | Asynchronous sends with callbacks, or batch-level waiting |
| Committing before processing | Records lost on a crash | Commit after processing |
| Ignoring consumer lag | Problems are found by users, not by alerts | Alert on lag and its growth rate |
| Auto topic creation in production | Topics with default partitions, RF, and retention appear from typos | `auto.create.topics.enable=false` |
| Sharing one `group.id` across different applications | They split the data between them | One group id per application |
| Long-running processing inside `poll()` loop without limits | Member removed from the group, rebalance loops | Bound the work per poll, or decouple polling from processing (as fs2-kafka does) |

## 35. Self-test

If you can answer these without looking, you understand Kafka well. Click to reveal the answers.

<details><summary>1. A producer uses <code>acks=all</code> on a topic with RF 3 and <code>min.insync.replicas=2</code>. Two brokers die. What happens to writes and reads?</summary>

Writes to partitions whose ISR is now 1 fail with `NotEnoughReplicas`, because the ISR is below 2. Reads still work from the remaining leader (if it was in the ISR). The cluster chooses availability for reads and consistency for writes.
</details>

<details><summary>2. Why can a consumer never read a record that is later lost in a leader failover?</summary>

Consumers read only below the high watermark, and records below the high watermark are on every in-sync replica. A new leader is elected from the ISR, so it has all those records. Only records above the high watermark can be truncated.
</details>

<details><summary>3. A group has 12 consumers on a 10-partition topic. Lag is growing. Will adding 4 more consumers help?</summary>

No. Only 10 consumers can be active; 2 are already idle. Add partitions (understanding that keys will move), speed up processing, or process records within one partition in parallel where order allows.
</details>

<details><summary>4. What exactly does idempotence protect against, and what does it not?</summary>

It removes duplicates caused by producer retries within one producer session, per partition, using producer id + sequence numbers. It does not protect against an application that sends the same event twice, against duplicates after the producer restarts with a new id, or against duplicate processing on the consumer side.
</details>

<details><summary>4b. Then what makes "exactly once" possible?</summary>

Transactions: the consumer's offset commit and the producer's output writes are committed atomically in one transaction, and downstream consumers use `read_committed`. A restarted instance with the same `transactional.id` fences the old one.
</details>

<details><summary>5. Produce p99 latency jumped from 20 ms to 300 ms. RemoteTimeMs is high, LocalTimeMs is normal. Where do you look?</summary>

At the followers: the leader is waiting for replication. Check follower brokers' disks, GC, network, and `num.replica.fetchers`, and look for ISR shrinks.
</details>

<details><summary>6. Why does Kafka not fsync every write, and why is that still safe?</summary>

fsync per write would destroy throughput. Durability comes from replication: an acknowledged `acks=all` write is in memory on every ISR replica, usually in different racks or zones. Losing it requires all of them to fail before the OS flushes.
</details>

<details><summary>7. How does a consumer find its group coordinator, and what happens to the group if that broker dies?</summary>

`abs(hash(group.id)) % 50` picks a partition of `__consumer_offsets`. The leader of that partition is the coordinator. If it dies, a follower of that partition becomes leader, replays the partition to rebuild group state, and becomes the new coordinator. Clients rediscover it with `FindCoordinator`.
</details>

<details><summary>8. What does the KRaft active controller do when a broker stops heartbeating?</summary>

After `broker.session.timeout.ms` (9 s), it writes a `FenceBrokerRecord` and `PartitionChangeRecord`s that move leadership of that broker's partitions to other ISR members. Brokers apply those records from the metadata log, and clients get `NotLeaderOrFollower` and refresh metadata.
</details>

<details><summary>9. Why might offsets in a topic have gaps, so that offset 105 follows 103?</summary>

Transaction control markers (commit or abort) take an offset but are never returned to consumers. Compaction also removes records, leaving gaps.
</details>

<details><summary>10. You add a new broker to a busy cluster. A week later it holds almost no data. Why?</summary>

Kafka does not rebalance existing partitions automatically. Only new partitions are placed there. Use `kafka-reassign-partitions.sh` (with a throttle) or a tool like Cruise Control.
</details>

<details><summary>11. What is the difference between the classic and the KIP-848 rebalance protocol, in one sentence?</summary>

In the classic protocol, a client leader computes the assignment after every member rejoins behind a global barrier; in KIP-848, the broker computes it and moves each member's partitions individually, with no global barrier.
</details>

<details><summary>12. When does a consumer start at the beginning of a topic?</summary>

Only when its group has no committed offset for that partition (a new group, or offsets expired after `offsets.retention.minutes`), or the committed offset is out of range, and `auto.offset.reset=earliest`. Otherwise it continues from the committed offset. You can also seek explicitly.
</details>

## 36. Where to go next

### A learning path

1. **Run it.** Start a single KRaft node, create topics, produce and consume with the console tools. Use `kafka-dump-log.sh` on a segment to see batches.
2. **Break it.** Run three brokers, kill one during a produce load with `acks=all`, and watch the ISR, `UnderReplicatedPartitions`, and leader changes.
3. **Watch a rebalance.** Start two consumers in one group, kill one, and watch `kafka-consumer-groups.sh --describe --members` and the logs.
4. **Read the protocol.** Pick `ProduceRequest.json`, `FetchRequest.json`, and `ConsumerGroupHeartbeatRequest.json` (copied into `src/main/resources/message/`) and follow each field into the code.
5. **Measure.** Use the perf-test tools to find your own producer and consumer limits.
6. **Build failure tests** in this project for the cases listed in section 33.

### KIPs worth reading

KIPs (Kafka Improvement Proposals) explain *why* each feature works the way it does. Search for them at `cwiki.apache.org/confluence/display/KAFKA`.

| KIP | Topic |
|---|---|
| KIP-98 | Exactly-once: idempotence and transactions |
| KIP-101 | Leader epochs for safe truncation |
| KIP-392 | Fetch from the closest replica |
| KIP-405 | Tiered storage |
| KIP-500 / KIP-595 / KIP-631 | Replacing ZooKeeper: KRaft, the Raft protocol, the controller |
| KIP-848 | The new consumer rebalance protocol |
| KIP-853 | Dynamic KRaft quorums |
| KIP-890 | Transactions server-side defense |
| KIP-932 | Share groups (queues) |
| KIP-966 | Eligible leader replicas |
| KIP-996 | Pre-vote in KRaft |
| KIP-1071 | Streams groups |
