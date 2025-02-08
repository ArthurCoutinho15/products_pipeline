import sys
sys.path.append('/opt/airflow/external_scripts')

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from mongo_conn import Mongo


class LoadOperator(BaseOperator):
    def __init__(self, *args, **kwargs):
        pass