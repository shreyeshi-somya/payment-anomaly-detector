from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'anomaly_detection_pipeline',
    default_args=default_args,
    description='Daily anomaly detection pipeline',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    aggregate = BashOperator(
        task_id='aggregate_daily_metrics',
        bash_command='python /opt/airflow/src/aggregation/daily_metrics.py',
    )

    detect = BashOperator(
        task_id='detect_anomalies',
        bash_command='python /opt/airflow/src/anomaly_detection/anomaly_detection.py',
    )

    drilldown = BashOperator(
        task_id='run_drilldown',
        bash_command='python /opt/airflow/src/drilldown/drill_down.py',
    )

    aggregate >> detect >> drilldown