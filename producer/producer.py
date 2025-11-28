import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

SITES = ["CERN-PROD", "RAL-LCG2", "INFN-T1", "BNL-ATLAS", "PIC"]
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

while True:
    event = {
        "timestamp": datetime.utcnow().isoformat(),
        "src_site": random.choice(SITES),
        "dst_site": random.choice(SITES),
        "file_size_bytes": random.randint(10_000_000, 10_000_000_000),
        "success": random.random() > 0.1,   # 10% failure
    }

    producer.send("xrootd-transfers", event)
    print("Produced:", event)
    time.sleep(1)
