from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'start_date': datetime(2023, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'hdfs_to_mysql',
    default_args=default_args,
    description='DAG to run a Python file',
    schedule_interval='0 * * * *',  # Runs every hour on the hour
)

run_python_file = BashOperator(
    task_id='run_hdfs_to_mysql_file',
    bash_command='/home/naya/miniconda3/bin/python /home/naya/final_project/hdfs_to_mysql.py',
    dag=dag,
)

run_python_file
