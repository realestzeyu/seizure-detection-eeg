# Seizure Detection EEG

A real-time EEG seizure detection pipeline using Kafka, Spark, and Delta Lake.

## Prerequisites

- Python 3.11
- Java (required for PySpark): `sudo apt install default-jdk`
- Docker and Docker Compose (for Kafka)

> **Windows only:** PySpark requires Hadoop's `winutils.exe` even when not using HDFS.
> 1. Download all the files in `hadoop-3.3.5/bin/` from https://github.com/cdarlint/winutils 
> 2. Place it at `C:\hadoop` (create one urself)
> 3. Set the environment variable: `HADOOP_HOME=C:\hadoop`

## Setup

### 1. Clone the repo
```bash
git clone https://github.com/realestzeyu/seizure-detection-eeg
cd seizure-detection-eeg
```

### 2. Create and activate a virtual environment
```bash
python3.11 -m venv venv
source venv/bin/activate
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. Download the data
This will take a long ass time due to physionet server being completely ass.
```bash
wget -r -N -c -np https://physionet.org/files/chbmit/1.0.0/chb01/ -P data/raw/
```
Where RAM is used:
- mne loads the chunk into RAM temporarily (you control this with CHUNK_SIZE)
- Kafka stores messages on disk (/var/lib/kafka/data inside Docker) but the OS caches hot data in RAM via page cache
- Spark JVM heap holds open window state + shuffle buffers
- Delta writes go to SSD, then RAM is freed

Where CPU is used:
- Producer: numpy slicing + JSON serialization (Python, single threaded)
- Spark: deserializing JSON, computing aggregations, writing Parquet (JVM, can use multiple cores)

Where SSD is used:
- Raw .edf files — read once per producer run
- Kafka log files — written as messages arrive, deleted after retention period
- Delta tables — final output, stays permanently