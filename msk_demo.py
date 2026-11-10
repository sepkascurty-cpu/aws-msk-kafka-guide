#!/usr/bin/env python3
"""Amazon MSK — IAM-auth producer + consumer-group consumer.

Requires: kafka-python, aws-msk-iam-sasl-signer, boto3
    pip install kafka-python aws-msk-iam-sasl-signer boto3

Run against a real cluster:
    python msk_demo.py produce --bootstrap b-1.cluster.xxx.kafka.ap-southeast-1.amazonaws.com:9098
    python msk_demo.py consume --bootstrap b-1.cluster.xxx.kafka.ap-southeast-1.amazonaws.com:9098

Run offline (no cluster, no credentials) to see the flow end-to-end:
    python msk_demo.py --demo

Durability defaults baked in: acks=all, idempotent producer, keyed records
(ordering per key), consumer commits offsets only AFTER processing.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import uuid

TOPIC = "orders"
GROUP_ID = "orders-service"


# ---------------------------------------------------------------------------
# IAM auth (SASL/IAM) — the only auth MSK needs from AWS-native clients.
# ---------------------------------------------------------------------------
def _iam_auth_token(bootstrap: str) -> str:
    """Generate a short-lived SigV4 token MSK accepts as a SASL password."""
    from aws_msk_iam_sasl_signer import MSKAuthTokenProvider  # lazy: demo mode skips it

    def _token() -> tuple[str, float]:
        token, expiry_ms = MSKAuthTokenProvider.generate_auth_token("ap-southeast-1")
        return token, expiry_ms / 1000.0

    return _token  # kafka-python calls it as oauth_cb


def _producer_kwargs(bootstrap: str) -> dict:
    return {
        "bootstrap_servers": bootstrap,
        "security_protocol": "SASL_SSL",
        "sasl_mechanism": "OAUTHBEARER",
        "sasl_oauth_token_provider": _iam_auth_token(bootstrap),
        # --- durability: the whole point of this demo ---
        "acks": "all",                 # wait for all in-sync replicas
        "enable_idempotence": True,    # retries never duplicate a record
        "retries": 5,
        "retry_backoff_ms": 500,
        "delivery_timeout_ms": 120_000,
        "value_serializer": lambda v: json.dumps(v).encode("utf-8"),
        "key_serializer": lambda k: k.encode("utf-8"),
    }


def _consumer_kwargs(bootstrap: str) -> dict:
    return {
        "bootstrap_servers": bootstrap,
        "security_protocol": "SASL_SSL",
        "sasl_mechanism": "OAUTHBEARER",
        "sasl_oauth_token_provider": _iam_auth_token(bootstrap),
        "group_id": GROUP_ID,
        "auto_offset_reset": "earliest",
        "enable_auto_commit": False,   # commit AFTER processing, never before
        "value_deserializer": lambda b: json.loads(b.decode("utf-8")),
        "key_deserializer": lambda b: b.decode("utf-8") if b else None,
        "isolation_level": 1,          # read_committed: skip aborted transactions
    }


# ---------------------------------------------------------------------------
# Producer
# ---------------------------------------------------------------------------
def produce(bootstrap: str, count: int = 5) -> None:
    from kafka import KafkaProducer

    producer = KafkaProducer(**_producer_kwargs(bootstrap))
    try:
        for i in range(count):
            order_id = f"order-{1000 + i}"
            record = {
                "order_id": order_id,
                "total": 50_000 + i * 25_000,
                "currency": "IDR",
                "items": i + 1,
            }
            # Key = order_id: same order always lands on the same partition,
            # so its events stay ordered. No key = round-robin = no ordering.
            future = producer.send(TOPIC, key=order_id, value=record)
            meta = future.get(timeout=30)
            print(f"sent {order_id} -> partition {meta.partition} offset {meta.offset}")
        producer.flush()
    finally:
        producer.close()


# ---------------------------------------------------------------------------
# Consumer (consumer group — scale by adding consumers up to partition count)
# ---------------------------------------------------------------------------
def consume(bootstrap: str, max_records: int = 10) -> None:
    from kafka import KafkaConsumer

    consumer = KafkaConsumer(TOPIC, **_consumer_kwargs(bootstrap))
    seen = 0
    try:
        for msg in consumer:
            process_order(msg.key, msg.value)
            consumer.commit()  # offset committed only after successful processing
            seen += 1
            if seen >= max_records:
                break
    finally:
        consumer.close()
    print(f"consumed {seen} record(s), offsets committed")


def process_order(key: str | None, value: dict) -> None:
    """Your business logic goes here — keep it idempotent.

    Consumers crash and rebalances happen; a record may be delivered twice.
    Guard side effects with the record key (e.g. a DynamoDB conditional
    write on order_id) so a redelivery is a no-op, not a double charge.
    """
    print(f"  processing {key}: total={value['total']} {value['currency']}")


# ---------------------------------------------------------------------------
# Offline demo — same flow, in-memory log, no cluster needed.
# ---------------------------------------------------------------------------
def demo() -> None:
    print("== MSK offline demo: in-memory topic, same producer/consumer rules ==")
    log: list[dict] = []          # the "topic": ordered, append-only
    offsets: dict[str, int] = {}  # the "consumer group": committed offsets

    def demo_produce(order_id: str, total: int) -> None:
        record = {"key": order_id, "value": {"total": total, "currency": "IDR"}}
        log.append(record)  # acks=all + idempotent: written once, durably
        print(f"sent {order_id} -> partition {hash(order_id) % 3} offset {len(log) - 1}")

    def demo_consume() -> None:
        start = offsets.get(GROUP_ID, 0)
        for idx in range(start, len(log)):
            rec = log[idx]
            process_order(rec["key"], rec["value"])
            offsets[GROUP_ID] = idx + 1  # commit AFTER processing
        print(f"consumed {len(log) - start} record(s), offsets committed: {offsets}")

    for i in range(5):
        demo_produce(f"order-{1000 + i}", 50_000 + i * 25_000)
    demo_consume()
    print("\nReplay check: reset offset to 0 and the group re-reads everything:")
    offsets[GROUP_ID] = 0
    demo_consume()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="MSK producer/consumer demo")
    parser.add_argument("--demo", action="store_true", help="offline in-memory demo")
    parser.add_argument("mode", nargs="?", choices=["produce", "consume"],
                        help="produce to or consume from a real cluster")
    parser.add_argument("--bootstrap", default="", help="MSK bootstrap brokers, e.g. b-1...:9098")
    parser.add_argument("--count", type=int, default=5, help="records to produce")
    args = parser.parse_args(argv)

    if args.demo:
        demo()
        return 0
    if not args.bootstrap:
        parser.error("--bootstrap is required unless --demo is used")
    if args.mode == "produce":
        produce(args.bootstrap, args.count)
    elif args.mode == "consume":
        consume(args.bootstrap, args.count)
    else:
        parser.error("choose a mode: produce or consume (or --demo)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
