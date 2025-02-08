import sys
sys.path.append('/opt/airflow/external_scripts')

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

class TransformOperator(BaseOperator):
    @apply_defaults
    def __init__(self,*args, **kwargs):
        super().__init__(self, *args, **kwargs)
    
    def execute(self, context):
        ti = context['ti']
        task_data = ti.xcom_pull(task_ids='extract_task')

        if not task_data:
            raise ValueError('Não foi possível puxar os dados da extração')
    
        etl = task_data['etl']
        data = task_data['data']
        
        return {'etl':etl, 'data':data}