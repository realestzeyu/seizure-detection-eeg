from kafka import KafkaProducer
import json
import mne
import numpy as np
import time

# Load an .edf file with mne
# Loop through each sample across all channels
# For each sample, build a JSON message
# Publish it to the eeg-raw Kafka topic
# Sleep between samples to match 256Hz

producer = KafkaProducer(
    bootstrap_servers="localhost:29092",  # connect to EXTERNAL listener cuz the producer is on host machine
    value_serializer=lambda v: json.dumps(v).encode(
        "utf-8"
    ),  # serialise data to json, then endcode to bytes cuz kafka only accepts bytes
)

patient_id = "chb01"
session_id = "03"
file_path = (
    f"data/raw/physionet.org/files/chbmit/1.0.0/chb01/{patient_id}_{session_id}.edf"
)
raw = mne.io.read_raw_edf(file_path, preload=True)  # load the .edf file with mne

# Data that we need to send
sampling_rate = raw.info["sfreq"]  # get the sampling rate (256Hz)
start_time_ms = int(raw.info["meas_date"].timestamp() * 1000)  # start time in ms

# loop structure should be sample by sample, across all channels. then sleep for 1/256s per sample
# for i in range(raw.n_times):
#     data, times = raw.get_data(
#         start=i, stop=i + 1, return_times=True
#     )  # dont overload memory cuz it crashed jn
#     # i represents the sample index
#     timestamp_ms = start_time_ms + int(i * 1000 / sampling_rate)
#     futures = []
#     for j, channels in enumerate(raw.ch_names):
#         datapoint = data[j][0]  # per sample, per channel datapoint
#         message = {
#             "patient_id": patient_id,
#             "timestamp_ms": timestamp_ms,
#             "channel": channels,
#             "voltage": float(datapoint),
#             "sample_index": i,
#         }
#         futures.append((channels, producer.send("eeg-raw", message)))  # async send to kafka
#         # print(f"Sent sample {i}, channel {channels}, value {float(datapoint)}")
#     # time.sleep(1 / sampling_rate)

CHUNK_SIZE = 2560 

for chunk_start in range(0, raw.n_times, CHUNK_SIZE):
    chunk_end = min(chunk_start + CHUNK_SIZE, raw.n_times)
    data_chunk, times_chunk = raw.get_data(start=chunk_start, stop=chunk_end, return_times=True)
    timestamp_ms_chunk = start_time_ms + (times_chunk * 1000).astype(int)
    for k in range(chunk_end - chunk_start):
        for j, channel in enumerate(raw.ch_names):
            message = {
                "patient_id": patient_id,
                "timestamp_ms": int(timestamp_ms_chunk[k]),
                "channel": channel,
                "voltage": float(data_chunk[j][k]),
                "sample_index": chunk_start + k,
            }
            producer.send("eeg-raw", message)
    # time.sleep(CHUNK_SIZE / sampling_rate)  # for real time simulation

producer.flush()  # flush all pending msgs to kafka to ensure msgs are sent before producer closes
print("MESSAGE SENT, i like feet", flush=True)
