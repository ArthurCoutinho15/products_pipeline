from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.email import EmailOperator
from airflow.sensors.filesystem import FileSensor
from airflow.models.variable import Variable
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import json
import os

default_args = {
        'depends_on_past': False,
        'email': ['arthurtelescoutinho@gmail.com'],
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=10)
        
    }


with DAG('windturbine', description='Dag para processar dados de uma turbina eólica', 
         schedule_interval=None,
         start_date=datetime(2025, 2, 5),
         catchup=False,
         default_args=default_args,
         default_view='graph',
         tags=['Wind'],
         doc_md='## DAG para processar dados de turbina eólica') as dag:
    
    
    group_check_temp = TaskGroup('group_check_temp')
    
    file_sensor_task = FileSensor(
        task_id='file_sensor_task',
        filepath= Variable.get('path_file'),
        fs_conn_id= 'fs_default',
        poke_interval= 10,
    )
    
    def process_file(**kwargs):
        with open(Variable.get('path_file')) as f:
            data = json.load(f)
            kwargs['ti'].xcom_push(key='idtemp', value=data['idtemp'])
            kwargs['ti'].xcom_push(key='powerfactor', value=data['powerfactor'])
            kwargs['ti'].xcom_push(key='hydraulicpressure', value=data['hydraulicpressure'])
            kwargs['ti'].xcom_push(key='temperature', value=data['idtemperaturetemp'])
            kwargs['ti'].xcom_push(key='timestamp', value=data['timestamp'])
        os.remove(Variable.get('path_file'))
    
    
    
    
    get_data = PythonOperator(task_id='get_data', python_callable=process_file, provide_context = True)
    
    def avalia_temp(**kwargs):
        number = kwargs['ti'].xcom_pull(task_id='get_data', key='temperature')
        if number >= 24:
            return 'group_check_temp.send_email_alert'
        else:
            return 'group_check_temp.send_email_normal'
        
    
    send_email_alert = EmailOperator(task_id='send_email_alert', to='arthurtelescoutinho@gmail.com', subject='Airflow Alert', 
                                     html_content="""
                                                    <h1>Alerta de temperatura</h1>
                                                    <p>Dag: windturbine</p>
                                                   """,
                                    task_group=group_check_temp)
    
    send_email_normal = EmailOperator(task_id='send_email_normal', to='arthurtelescoutinho@gmail.com', subject='Airflow Alert', 
                                     html_content="""
                                                    <h1>Temperatura normal</h1>
                                                    <p>Dag: windturbine</p>
                                                   """,
                                    task_group=group_check_temp)
    
    check_temp_branc = BranchPythonOperator(task_id='check_temp_branc', python_callable=avalia_temp, provide_context=True, task_group=group_check_temp)
    
    with group_check_temp:
        check_temp_branc >> [send_email_alert, send_email_normal]
    
    
    file_sensor_task >> get_data
    get_data >> group_check_temp