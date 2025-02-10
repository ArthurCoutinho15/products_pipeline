from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys

sys.path.append("/opt/airflow/external_scripts")

from main import mongo_conn, extract, transform, load_data_into_mongo  

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 2, 8),
    "catchup": False
}

def run_etl():
    """ Função que executa todo o pipeline """
    collection = mongo_conn()
    etl, data = extract()
    data = transform(etl, data)
    load_data_into_mongo(etl, data, collection)

with DAG(
    dag_id="etl_products",
    description="DAG para rodar o script ETL completo",
    schedule_interval=None,
    default_args=default_args,
    tags=["ETL"]
) as dag:

    run_etl_task = PythonOperator(
        task_id="run_etl_task",
        python_callable=run_etl
    )

    run_etl_task
