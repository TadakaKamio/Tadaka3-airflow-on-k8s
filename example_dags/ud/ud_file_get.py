import inspect
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

dag_id = "ud_file_get"

def MyAppLog():
    # Get the caller's filename
    caller_frame = inspect.stack()[1]
    caller_file = caller_frame.filename
    
    print(f"MyAppLog was imported by: {caller_file}")
    print("Hello from MyAppLog function!")
