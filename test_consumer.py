from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "eeg-raw",  # topic
    bootstrap_servers="localhost:29092",  # consumer is on host machine as well, so connect to EXTERNAL listener
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",  # start from the earliest msg
    consumer_timeout_ms=5000,
)

# quick test to consume msgs
for message in consumer:
    print(message.value)
    break  # remove this to keep listening
