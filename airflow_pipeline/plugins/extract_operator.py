import sys
sys.path.append('/opt/airflow/external_scripts')

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from etl import Etl

class ExtractOperator(BaseOperator):
    @apply_defaults
    def __init__(self, url,*args, **kwargs):
        super().__init__(*args, **kwargs)
        self.url = url
        
    def execute(self, context):
        etl = Etl(self.url)
        data = etl.extract_data()
        
        return {'etl': etl, 'data': data}
        
