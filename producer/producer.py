import json
import time
import random
from datetime import datetime

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

SITES = ["CERN-PROD", "RAL-LCG2", "INFN-T1", "BNL-ATLAS", "PIC"]


def create_producer():
    """Retry until Kafka is reachable."""
    while True:
        try:
            print("Trying to connect to Kafka at localhost:9092 ...")
            producer = KafkaProducer(
                bootstrap_servers="localhost:9092",
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                # Avoid auto-version detection which can throw NoBrokersAvailable
                api_version=(0, 10),  # old but universally supported for our simple use-case
            )
            print("✅ Connected to Kafka.")
            return producer
        except NoBrokersAvailable as e:
            print(f"❌ No brokers available yet: {e}. Retrying in 3s ...")
            time.sleep(3)


producer = create_producer()

while True:
    event = {
        "timestamp": datetime.utcnow().isoformat(),
        "src_site": random.choice(SITES),
        "dst_site": random.choice(SITES),
        "file_size_bytes": random.randint(10_000_000, 10_000_000_000),
        "success": random.random() > 0.1,  # 10% failure
    }

    future = producer.send("xrootd-transfers", event)
    try:
        record_md = future.get(timeout=10)
        print(f"Produced to {record_md.topic}[{record_md.partition}]@{record_md.offset}: {event}")
    except Exception as e:
        print(f"⚠️ Send failed: {e}")

    time.sleep(1)
