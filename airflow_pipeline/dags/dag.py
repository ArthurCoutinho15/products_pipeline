import sys
sys.path.append('/opt/airflow/external_scripts')

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.email import EmailOperator
from airflow.sensors.filesystem import FileSensor
from airflow.models.variable import Variable
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import json
import os
from hello_world import hello_world

default_args = {
        'depends_on_past': False,
        'email': ['arthurtelescoutinho@gmail.com'],
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=10)
        
    }


with DAG('hello_world', description='Dag para processar dados de uma turbina eólica', 
         schedule_interval=None,
         start_date=datetime(2025, 1, 5),
         catchup=False,
         default_args=default_args,
         default_view='graph',
         tags=['Wind'],
         doc_md='## DAG para processar dados de turbina eólica') as dag:
    
    task1 = PythonOperator(task_id='tsk1', python_callable=hello_world)