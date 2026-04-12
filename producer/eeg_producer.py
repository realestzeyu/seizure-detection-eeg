from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers="localhost:29092",  # connect to EXTERNAL listener cuz the producer is on host machine
    value_serializer=lambda v: json.dumps(v).encode(
        "utf-8"
    ),  # serialise data to json, then endcode to bytes cuz kafka only accepts bytes
)

# example msg
message = {
    "patient_id": "chb01",
    "timestamp_ms": 1700000000000,
    "channel": "FP1-F7",
    "microvolt_value": -45.23,
    "sample_index": 12345,
}

future = producer.send("eeg-raw", message)
result = future.get(timeout=10)
producer.flush()
print("MESSAGE SENT", result)
