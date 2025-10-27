import inspect
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

dag_id = "ud_file_get"
with DAG(dag_id=dag_id, start_date=datetime(2025, 10, 27),
         schedule_interval=None) as dag:
                  
    def MyAppLog():
    # Get the caller's filename
        caller_frame = inspect.stack()[1]
        caller_file = caller_frame.filename

        print(f"MyAppLog was imported by: {caller_file}")
        print("Hello from MyAppLog function!")
