from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "batch"))
from eeg_daily_aggregator import main as batch_main

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "quality"))
from run_validation import main as validation_main

with DAG(
    dag_id="begin_ingestion",
    start_date=datetime(2026, 4, 23),
    schedule=timedelta(days=1),
    catchup=False,
) as dag:

    run_batch_aggregator = PythonOperator(
        task_id="aggregate",
        python_callable=batch_main,  # need to import the main function from batch/eeg_daily_aggregator.py and call it here
        op_kwargs={"patient_id": "chb01"},
    )

    run_validation = PythonOperator(
        task_id="validation",
        python_callable=validation_main,  # need to import the main function from quality/run_validation.py and call it here
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="/home/zeyu/seizure-detection-eeg/venv/bin/dbt run --no-partial-parse --project-dir /home/zeyu/seizure-detection-eeg/dbt --profiles-dir /home/zeyu/seizure-detection-eeg/dbt",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="/home/zeyu/seizure-detection-eeg/venv/bin/dbt test --no-partial-parse --project-dir /home/zeyu/seizure-detection-eeg/dbt --profiles-dir /home/zeyu/seizure-detection-eeg/dbt",
    )

    run_batch_aggregator >> run_validation >> dbt_run >> dbt_test
