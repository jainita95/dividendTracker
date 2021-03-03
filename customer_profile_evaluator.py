from airflow.operators.python_operator import PythonOperator,BranchPythonOperator,PythonVirtualenvOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator  import BashOperator
from airflow.hooks.base_hook import BaseHook
from airflow.models import DAG
import datetime
from datetime import datetime, timedelta
import sys
import os
sys.path.insert(0,'/opt/bitnami/airflow/dags/git-github-com-jainita95-dividend-tracker-git')
from scripts.subfolder.train_model_with_hitoric_data import train_model_and_store 
from scripts.subfolder.predict_profile_for_every_customer import predict_profile 
from scripts.subfolder.update_data_warehouse import update_data_warehouse
#==================DAG ARGUMENTS==============================



args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}
dag = DAG(dag_id='customer-profile-evaluator', default_args=args, schedule_interval=timedelta(days=1))  
is_train_model = True
is_predict_profile = True
is_upadate_warehouse = False

def check_train_api():
    if is_train_model:
        return 'TrainModel'
    else:
        return 'Skip1'

def check_predict_profile():
    if is_predict_profile:
        return 'PredictProfile'
    else:
        return 'Skip2'
    
def check_update_warehouse():
    if is_upadate_warehouse:
        return 'UpdateWarehouse'
    else:
        return 'Skip3'    

    
CheckTrainApi = BranchPythonOperator(
    task_id='CheckTrainApi',
    python_callable=check_train_api,
     trigger_rule='all_done',
    dag=dag
)
CheckPredictProfile = BranchPythonOperator(
    task_id='CheckPredictProfile',
    python_callable=check_predict_profile,
    trigger_rule='all_done',
    dag=dag
)

CheckUpdateWarehouse = BranchPythonOperator(
    task_id='CheckUpdateWarehouse',
    python_callable=check_update_warehouse,
    trigger_rule='all_done',
    dag=dag
)
#python3 /home/airflow/gcs/dags/test.py
TrainModel = PythonVirtualenvOperator(
    task_id='TrainModel',
    python_callable=update_data_warehouse,
    requirements=['sendgrid==6.4.8','apache-airflow','psycopg2-binary','google-cloud-bigquery','google-cloud-bigquery-storage','pandas','pyarrow','datetime','pandas_gbq','tqdm','google-cloud-storage','sklearn','cloudstorage'],
    python_version='3',
    trigger_rule='all_done',
    dag=dag
)
PredictProfile = PythonVirtualenvOperator(
    task_id='PredictProfile',
    python_callable=train_model_and_store,
    requirements=['sendgrid==6.4.8','apache-airflow','psycopg2-binary','google-cloud-bigquery','google-cloud-bigquery-storage','pandas','pyarrow','datetime','pandas_gbq','tqdm','google-cloud-storage','fsspec', 'sklearn','gcsfs', 'cloudstorage'],
    python_version='3',
    trigger_rule='all_done',
    dag=dag
)
UpdateWarehouse = PythonVirtualenvOperator(
    task_id='UpdateWarehouse',
    python_callable=predict_profile,
    requirements=['sendgrid==6.4.8','apache-airflow','psycopg2-binary','google-cloud-bigquery','google-cloud-bigquery-storage','pandas','pyarrow','datetime','pandas_gbq','tqdm','google-cloud-storage', 'sklearn', 'cloudstorage'],
    python_version='3',
    trigger_rule='all_done',
    dag=dag
)


Join = DummyOperator(task_id='Join', dag=dag,trigger_rule='all_done')
Skip1 = DummyOperator(task_id='Skip1', dag=dag,trigger_rule='all_done')
Skip2 = DummyOperator(task_id='Skip2', dag=dag,trigger_rule='all_done')
Skip3 = DummyOperator(task_id='Skip3', dag=dag,trigger_rule='all_done')
TrainModel.set_upstream(CheckTrainApi)
Skip1.set_upstream(CheckTrainApi)
CheckPredictProfile.set_upstream(Skip1)
CheckPredictProfile.set_upstream(TrainModel)
PredictProfile.set_upstream(CheckPredictProfile)
Skip2.set_upstream(CheckPredictProfile)
CheckUpdateWarehouse.set_upstream(Skip2)
CheckUpdateWarehouse.set_upstream(PredictProfile)
UpdateWarehouse.set_upstream(CheckUpdateWarehouse)
Skip3.set_upstream(CheckUpdateWarehouse)
Join.set_upstream(Skip3)
Join.set_upstream(UpdateWarehouse)
#PredictProfile.set_upstream(TrainModel)
#UpdateWarehouse.set_upstream(PredictProfile)
