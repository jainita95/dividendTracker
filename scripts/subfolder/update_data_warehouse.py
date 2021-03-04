def update_data_warehouse():
    from airflow.operators.python_operator import PythonOperator
    from airflow.operators.bash_operator  import BashOperator
    from airflow.hooks.base_hook import BaseHook
    from airflow.models import DAG
    from airflow.utils import dates
    from datetime import datetime, timedelta, date
    from google.cloud import bigquery
    from google.oauth2 import service_account
    import requests
    import json
    import pandas_gbq as pdgbq
    from datetime import date
    from datetime import datetime,timedelta
    import numpy as np
    import pandas as pd
    import json
    import os
    from airflow.models import Variable
    from urllib3.util.retry import Retry
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    from requests.adapters import HTTPAdapter
    from pandas.api.types import is_datetime64_any_dtype as is_datetime
    from sendgrid import SendGridAPIClient
    from sendgrid.helpers.mail import Mail
    from string import Template
    from sendgrid.helpers.mail import To,Attachment,FileContent,FileType,FileName,Disposition,ContentId
    import base64
    from pandas.io.json import json_normalize
    import math
    from google.cloud import storage
    import numpy as np
    import pandas as pd
    from sklearn.linear_model import LogisticRegression  
    from sklearn.neighbors import KNeighborsClassifier
    from sklearn.svm import SVC
    from sklearn.tree import DecisionTreeClassifier 
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.model_selection import train_test_split
    from sklearn import preprocessing
    from sklearn.model_selection import KFold
    from sklearn.model_selection import cross_val_score, GridSearchCV,train_test_split,cross_val_score
    import itertools
    from sklearn.preprocessing import PolynomialFeatures
    from sklearn.neighbors import LocalOutlierFactor # çok değişkenli aykırı gözlem incelemesi
    from sklearn.preprocessing import scale,StandardScaler, MinMaxScaler,Normalizer,RobustScaler
    from sklearn.preprocessing import StandardScaler
    from sklearn.metrics import classification_report
    from sklearn.metrics import  accuracy_score, f1_score, precision_score,confusion_matrix, recall_score, roc_auc_score
    from sklearn.ensemble import RandomForestClassifier,AdaBoostClassifier,GradientBoostingClassifier
    import warnings
    from google.cloud import storage
    from google.oauth2 import service_account
    from sklearn.preprocessing import LabelEncoder
    from sklearn.preprocessing import StandardScaler
    import pickle
    import os
    import cloudstorage as gcs
    from google.cloud import bigquery
    import json
    warnings.filterwarnings("ignore", category=DeprecationWarning) 
    warnings.filterwarnings("ignore", category=FutureWarning) 
    warnings.filterwarnings("ignore", category=UserWarning) 
    source_bucket = 'hackathon-21-customer-profile-details-temp'
    storage_client = storage.Client()
    source_bucket = storage_client.bucket(source_bucket)
    credentials=service_account.Credentials.from_service_account_info(
               Variable.get("key",deserialize_json=True))
    filepaths_inward = []
    fnames_inward = []
    updated_inward = []
    dict_df = {}


    for file in list(source_bucket.list_blobs()):
        file_path='gs://{}/{}'.format(file.bucket.name, file.name)
        if(file.name.endswith(".csv")):
            filepaths_inward.append(file_path)
            fnames_inward.append(file.name)
            updated_inward.append(file.updated)
    print(filepaths_inward)

    df_file_inward = pd.DataFrame(fnames_inward, columns=['fname'])
    df_file_inward['filepath'] = filepaths_inward
    df_file_inward['updated'] = pd.to_datetime(updated_inward)
    df_file_inward['updated'] = df_file_inward['updated'].dt.date
    if df_file_inward.empty:
        raise ValueError("No Content to process")
    dict_df['df_inward'] = df_file_inward.to_json()
    dependent_variable_name = "Exited"
    table_schema=[{'name': 'CustomerId', 'type': 'STRING', 'mode': 'REQUIRED'}, 
                         {'name': 'Surname', 'type': 'STRING', 'mode': 'NULLABLE'},
                         {'name': 'RM_ID', 'type': 'STRING', 'mode': 'NULLABLE'},
                         {'name': 'RM_mailId', 'type': 'STRING', 'mode': 'NULLABLE'},
                             {'name': 'CreditScore', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                             {'name': 'Geography', 'type': 'STRING', 'mode': 'NULLABLE'},
                             {'name': 'Gender', 'type': 'STRING', 'mode': 'NULLABLE'},
                         {'name': 'Age', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                         {'name': 'Tenure', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                         {'name': 'Balance', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                         {'name': 'NumOfProducts', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                         {'name': 'HasCrCard', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                         {'name': 'IsActiveMember', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                         {'name': 'EstimatedSalary', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                 {'name': 'Errorlogs', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                 {'name': 'Exited', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                 {'name': 'Contacts_Phone', 'type': 'STRING', 'mode': 'NULLABLE'},
                 {'name': 'Contacts_email', 'type': 'STRING', 'mode': 'NULLABLE'},
                 {'name': 'isCreditScoreFactor', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
                 {'name': 'isAgeFactor', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
                 {'name': 'isBalanceFactor', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
                 {'name': 'isNumOfProductsFactor', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
                 {'name': 'isHasCrCardFactor', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
                 {'name': 'isActiveMemberFactor', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
                 {'name': 'isErrorLogsFactor', 'type': 'BOOLEAN', 'mode': 'NULLABLE'}]

    project_id = 'hackathon-wpb'
    dataset_id = 'customer_profiles'
    table_id = 'customer_profiles_europe'

    client  = bigquery.Client(project = project_id)
    dataset  = client.dataset(dataset_id)
    table = dataset.table(table_id)

    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.schema = table_schema
    job_config.write_disposition = 'WRITE_TRUNCATE'

    if len(filepaths_inward) > 0:
        df = pd.concat((pd.read_csv(f) for f in filepaths_inward)) 
        df.drop(['Unnamed: 0', 'RowNumber'], axis=1, inplace=True)
        convert_dict = {'Age': int, 
                            'Tenure': int,
                        'NumOfProducts': int,
                        'HasCrCard': int,
                        'IsActiveMember': int,
                        'Errorlogs': int,
                        'CustomerId': int,
                        'RM_ID': int,
                        'Contact_Phone': int
                           } 

        df = df.astype(convert_dict)
        convert_dict = {
                        'CustomerId': str,
                        'RM_ID': str,
                        'Contact_Phone': str
                           }

        df = df.astype(convert_dict)
        df.rename(columns={"Contact_Phone": "Contacts_Phone"}, inplace=True)
        df.rename(columns={"Contact_MailId": "Contacts_email"}, inplace=True)
        json_data = df.to_json(orient="records")
        json_object = json.loads(json_data)
        job = client.load_table_from_json(json_object, table, job_config = job_config)
        print(job.result())
