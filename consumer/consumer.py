from kafka import KafkaConsumer
from prometheus_client import start_http_server, Counter, Gauge
import json
import time

consumer = KafkaConsumer(
    "xrootd-transfers",
    bootstrap_servers="kafka:9092",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

bytes_per_site = Counter("xrootd_bytes", "Bytes transferred", ["site"])
failures = Counter("xrootd_failures", "Failed transfers", ["site"])

start_http_server(8000)

for msg in consumer:
    event = msg.value
    site = event["src_site"]
    bytes_per_site.labels(site=site).inc(event["file_size_bytes"])

    if not event["success"]:
        failures.labels(site=site).inc()

    print("Processed:", event)
