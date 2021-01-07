from airflow.operators.python_operator import PythonOperator,BranchPythonOperator,PythonVirtualenvOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator  import BashOperator
from airflow.hooks.base_hook import BaseHook
from airflow.models import DAG
import datetime
from datetime import datetime, timedelta
import sys
import os
sys.path.insert(0,os.path.abspath(os.path.dirname('/scripts')))
from scripts.subfolder.update_csv_data_to_bq import csv_load 
from scripts.subfolder.dividend_processor import calculate_probability 
from scripts.subfolder.load_dividend_info import call_dividend_api
#==================DAG ARGUMENTS==============================



args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}
dag = DAG(dag_id='dividend_orchestrator', default_args=args, schedule_interval=timedelta(days=1))  
is_call_api = False
is_csv_load = True
is_calculate_probability = True

def check_call_api():
    if is_call_api:
        return 'CallDividendApi'
    else:
        return 'Skip1'

def check_csv_load():
    if is_csv_load:
        return 'CsvLoad'
    else:
        return 'Skip2'
    
def check_calculate_probability():
    if is_calculate_probability:
        return 'CalculateProbability'
    else:
        return 'Skip3'    

    
CheckCallApi = BranchPythonOperator(
    task_id='CheckCallApi',
    python_callable=check_call_api,
     trigger_rule='all_done',
    dag=dag
)
CheckCsvLoad = BranchPythonOperator(
    task_id='CheckCsvLoad',
    python_callable=check_csv_load,
    trigger_rule='all_done',
    dag=dag
)

CheckCalculateProbability = BranchPythonOperator(
    task_id='CheckCalculateProbability',
    python_callable=check_calculate_probability,
    trigger_rule='all_done',
    dag=dag
)
#python3 /home/airflow/gcs/dags/test.py
CallDividendApi = PythonVirtualenvOperator(
    task_id='CallDividendApi',
    python_callable=call_dividend_api,
    requirements=['sendgrid==6.4.8'],
    trigger_rule='all_done',
    dag=dag
)
CsvLoad = PythonVirtualenvOperator(
    task_id='CsvLoad',
    python_callable=csv_load,
    requirements=['sendgrid==6.4.8'],
    trigger_rule='all_done',
    dag=dag
)
CalculateProbability = PythonVirtualenvOperator(
    task_id='CalculateProbability',
    python_callable=calculate_probability,
    requirements=['sendgrid==6.4.8'],
    trigger_rule='all_done',
    dag=dag
)


Join = DummyOperator(task_id='Join', dag=dag,trigger_rule='all_done')
Skip1 = DummyOperator(task_id='Skip1', dag=dag,trigger_rule='all_done')
Skip2 = DummyOperator(task_id='Skip2', dag=dag,trigger_rule='all_done')
Skip3 = DummyOperator(task_id='Skip3', dag=dag,trigger_rule='all_done')
CallDividendApi.set_upstream(CheckCallApi)
Skip1.set_upstream(CheckCallApi)
CheckCsvLoad.set_upstream(Skip1)
CheckCsvLoad.set_upstream(CallDividendApi)
CsvLoad.set_upstream(CheckCsvLoad)
Skip2.set_upstream(CheckCsvLoad)
CheckCalculateProbability.set_upstream(Skip2)
CheckCalculateProbability.set_upstream(CsvLoad)
CalculateProbability.set_upstream(CheckCalculateProbability)
Skip3.set_upstream(CheckCalculateProbability)
Join.set_upstream(Skip3)
Join.set_upstream(CalculateProbability)
#CsvLoad.set_upstream(CallDividendApi)
#CalculateProbability.set_upstream(CsvLoad)
