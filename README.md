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
