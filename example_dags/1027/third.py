from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

dag_id = "A_third_dag_from_config_dag_folder"

with DAG(dag_id=dag_id, start_date=datetime(2018, 11, 14),
         schedule_interval=None) as dag:

    def say_hello():
        print("Hello, guys! Your are the best!")

    PythonOperator(task_id="say_hello", python_callable=say_hello)
