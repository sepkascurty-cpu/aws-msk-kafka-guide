# Quiz — Amazon MSK

Test what stuck from the guide. Answers are collapsed below each question.

---

## 1. Your cluster has 3 brokers and a topic with 6 partitions, replication factor 3. A consumer group has 9 consumers. How many actually process data?

A. All 9 — Kafka load-balances evenly across every consumer
B. 6 — one consumer per partition; the other 3 sit idle
C. 3 — one consumer per broker
D. 9, but each record is processed 1.5 times on average

<details>
<summary>Answer</summary>

**B.** A partition is assigned to exactly one consumer in a group, so at most 6 consumers get work. Extra consumers idle until you add partitions (or a consumer dies and a rebalance hands its partitions over). Rule: consumers ≤ partitions, or you're paying for idle fleet.

</details>

---

## 2. What happens to ordering if you produce records with no key?

A. Nothing — Kafka preserves global order across the whole topic
B. Records are ordered by timestamp instead
C. Ordering is lost — records round-robin across partitions, so related events can arrive out of order
D. The producer fails with an error

<details>
<summary>Answer</summary>

**C.** Ordering in Kafka is *per partition*, and the partition is chosen from the key. With no key, the default partitioner spreads records round-robin. If event order matters (e.g. order created → paid → shipped), key by the entity ID (`order_id`) so all its events land on the same partition.

</details>

---

## 3. Which producer configuration actually makes a write durable against one broker failing?

A. `acks=1`
B. `acks=all` + topic `min.insync.replicas=2` (with replication factor 3)
C. `acks=0` — fastest, and Kafka retries in the background
D. `enable.idempotence=true` alone

<details>
<summary>Answer</summary>

**B.** `acks=all` means the broker waits until every in-sync replica acknowledges; `min.insync.replicas=2` means the write only counts when at least 2 replicas have it. With RF=3 that survives one broker dying. `acks=1` only waits for the leader — if the leader dies before replicating, the record is gone. Idempotence prevents *duplicates* on retry, not data loss.

</details>

---

## 4. Your consumer lag alarm fires and keeps growing. What's the FIRST thing to check?

A. Add more consumers to the group immediately
B. Whether the consumer count already equals the partition count
C. Increase the topic retention to 30 days
D. Restart the brokers

<details>
<summary>Answer</summary>

**B.** If consumers already equal partitions, adding more does nothing — they will idle (see Q1). The fix is to add partitions (and rebalance), fix a slow consumer (slow processing, bad deserialization, downstream bottleneck), or scale the downstream. Restarting brokers or raising retention treats the symptom, not the cause.

</details>

---

## 5. A consumer crashes after processing a record but BEFORE committing its offset. On restart, what happens — and what should your code do about it?

A. The record is lost forever; nothing you can do
B. The record is redelivered, so processing must be idempotent (e.g. a conditional write keyed on the record ID)
C. Kafka detects the crash and skips the record automatically
D. The consumer group rebalances and the record is assigned to a different topic

<details>
<summary>Answer</summary>

**B.** Offsets are committed *after* processing, so a crash before commit means the record is delivered again on restart. That's by design — at-least-once delivery. Your processing must be idempotent: key side effects on a stable record ID (e.g. `order_id`) with a conditional write, so a redelivery is a no-op instead of a double charge. Committing *before* processing would avoid the duplicate — and lose the record instead. Don't.

</details>
