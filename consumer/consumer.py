import json
import time

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from prometheus_client import start_http_server, Counter

TOPIC = "xrootd-transfers"
BOOTSTRAP_SERVERS = "kafka:29092"  # internal Kafka listener from docker-compose

bytes_per_site = Counter("xrootd_bytes", "Bytes transferred", ["site"])
failures = Counter("xrootd_failures", "Failed transfers", ["site"])


def create_consumer():
    """Retry until Kafka is reachable from inside Docker."""
    while True:
        try:
            print(f"🔌 Trying to connect to Kafka at {BOOTSTRAP_SERVERS} ...")
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                # avoid check_version() probing that can trigger NoBrokersAvailable
                api_version=(0, 10),
            )
            print(f"✅ Connected and subscribed to topic '{TOPIC}'.")
            return consumer
        except NoBrokersAvailable as e:
            print(f"❌ No brokers available yet: {e}. Retrying in 3s ...")
            time.sleep(3)


def main():
    print("✅ Starting Prometheus HTTP server on :8000")
    start_http_server(8000)

    consumer = create_consumer()
    print("📥 Waiting for messages...")

    for msg in consumer:
        event = msg.value
        site = event["src_site"]

        bytes_per_site.labels(site=site).inc(event["file_size_bytes"])
        if not event["success"]:
            failures.labels(site=site).inc()

        print("Consumed:", event)


if __name__ == "__main__":
    main()
