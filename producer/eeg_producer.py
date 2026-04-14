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

future = producer.send(
    "eeg-raw", message
)  # async send, returns a future that can check if msg is sent or not
result = future.get(
    timeout=10
)  # wait for the send to complete, timeout 10s so that u dont wait forever
producer.flush()  # flush all pending msgs to kafka to ensure msgs are sent before producer closes
print("MESSAGE SENT, i like feet", result)
