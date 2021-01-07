from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator  import BashOperator
from airflow.hooks.base_hook import BaseHook
from airflow.models import DAG
from airflow.utils import dates
from datetime import datetime, timedelta, date
from google.cloud import bigquery
from google.oauth2 import service_account
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
from google.cloud import storage
import io
import pathlib
import numpy as np
from pandas.io.json import json_normalize
import tempfile

def csv_load():
    source_bucket = 'csv_triggered_dataflow'
    destination_bucket = 'csv_triggered_dataflow_processed'
    storage_client = storage.Client()
    source_bucket = storage_client.bucket(source_bucket)
    destination_bucket = storage_client.bucket(destination_bucket)
    credentials=service_account.Credentials.from_service_account_info(
           Variable.get("key",deserialize_json=True))

    project_id = 'hackathon-wpb'
    query_string = """
            SELECT *
            FROM `customer_relations.customer_dividend_malaysia`"""

    table_schema=[{'name': 'Ticker', 'type': 'STRING', 'mode': 'REQUIRED'}, 
                 {'name': 'Mic', 'type': 'STRING', 'mode': 'REQUIRED'},
                 {'name': 'Contacts', 'type': 'RECORD', 'mode': 'REPEATED', 'fields': [
                     {'name': 'Name', 'type': 'STRING', 'mode': 'NULLABLE'},
                     {'name': 'email', 'type': 'STRING', 'mode': 'NULLABLE'}]},
                 {'name': 'Dividend', 'type': 'RECORD', 'mode': 'REPEATED', 'fields': [
                     {'name': 'DeclarationYear', 'type': 'STRING', 'mode': 'NULLABLE'},
                     {'name': 'DeclaratioMonth', 'type': 'STRING', 'mode': 'NULLABLE'},
                     {'name': 'DeclarationDate', 'type': 'STRING', 'mode': 'NULLABLE'}]},
                 {'name': 'RecentDeclarationDate', 'type': 'DATE', 'mode': 'NULLABLE'},
                 {'name': 'NextPayableDate', 'type': 'DATE', 'mode': 'NULLABLE'},
                 {'name': 'ExpectedStartDate', 'type': 'DATE', 'mode': 'NULLABLE'},
                 {'name': 'ExpectedEndDate', 'type': 'DATE', 'mode': 'NULLABLE'},
                 {'name': 'LastRunDate', 'type': 'DATE', 'mode': 'NULLABLE'},
                 {'name': 'ProbabilityNextMonthDeclaration', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
                 {'name': 'Period', 'type': 'INTEGER', 'mode': 'NULLABLE'}]

    project_id = 'hackathon-wpb'
    dataset_id = 'customer_relations'
    table_id = 'customer_dividend_malaysia'

    client  = bigquery.Client(project = project_id)
    dataset  = client.dataset(dataset_id)
    table = dataset.table(table_id)

    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.schema = table_schema
    job_config.write_disposition = 'WRITE_TRUNCATE'

    filepaths_outward = []
    fnames_outward = []
    updated_outward = []

    filepaths_inward = []
    fnames_inward = []
    updated_inward = []
    dict_df = {}
    for file in list(source_bucket.list_blobs()):
        file_path='gs://{}/{}'.format(file.bucket.name, file.name)
        if(file.name.startswith('Swift_outward')  and file.name.endswith(".csv")):
            filepaths_outward.append(file_path)
            fnames_outward.append(file.name)
            updated_outward.append(file.updated)
        elif(file.name.startswith('Swift_inward')  and file.name.endswith(".csv")):
            filepaths_inward.append(file_path)
            fnames_inward.append(file.name)
            updated_inward.append(file.updated)
    print(filepaths_outward)
    print(filepaths_inward)
    df_file_outward = pd.DataFrame(fnames_outward, columns=['fname'])
    df_file_outward['filepath'] = filepaths_outward
    df_file_outward['updated'] = pd.to_datetime(updated_outward)
    df_file_outward['updated'] = df_file_outward['updated'].dt.date

    df_file_inward = pd.DataFrame(fnames_inward, columns=['fname'])
    df_file_inward['filepath'] = filepaths_inward
    df_file_inward['updated'] = pd.to_datetime(updated_inward)
    df_file_inward['updated'] = df_file_inward['updated'].dt.date
    if df_file_outward.empty and df_file_inward.empty:
        raise ValueError("No Content to process")
    dict_df['df_outward'] = df_file_outward.to_json()
    dict_df['df_inward'] = df_file_inward.to_json()

    filepaths_outward = df_file_outward['filepath'] 
    filepaths_inward = df_file_inward['filepath'] 
    print(filepaths_outward)
    print(filepaths_inward)
    data = [['jainita', 'jainita95@gmail.com'], ['shivani', 'shivani173mankar@gmail.com']] 

    Contacts = pd.DataFrame(data, columns = ['Name', 'email']) 
    Contacts = json.loads(Contacts.to_json(orient="records"))
    print(type(Contacts))
    outward_Mic = 'HSBC_local_customer'
    inward_Mic = 'HSBC_non_local_customer'

    dataframe_bq = pdgbq.read_gbq(query = query_string,project_id=project_id)

    print(dataframe_bq.dtypes)
    print(len(dataframe_bq))
    if not filepaths_outward.empty:
        dataframe_csv_outward = pd.concat((pd.read_csv(f) for f in filepaths_outward))
        dataframe_csv_outward = dataframe_csv_outward[['Ordering Customer line 1 (Tag 50)','Calendar Date']]
        print(len(dataframe_csv_outward))
        dataframe_csv_outward = dataframe_csv_outward.drop_duplicates(subset=['Ordering Customer line 1 (Tag 50)','Calendar Date'])
        dataframe_csv_outward.rename(columns = {'Ordering Customer line 1 (Tag 50)':'Ticker'}, inplace = True)
        #print(dataframe_csv_outward.head())
        print(len(dataframe_csv_outward))
        unique_outward_ticker = pd.DataFrame(dataframe_csv_outward['Ticker'].unique())
        unique_outward_ticker.rename(columns = {0:'Ticker'}, inplace = True)
        unique_outward_ticker['Mic'] = outward_Mic
        unique_outward_ticker['Contacts'] = unique_outward_ticker.apply(lambda x: Contacts, axis=1)
        unique_outward_ticker['Dividend'] = unique_outward_ticker.apply(lambda x: [], axis=1)
        unique_outward_ticker['Period'] = 1
        unique_outward_ticker['RecentDeclarationDate'] = pd.NaT
        unique_outward_ticker['NextPayableDate'] = pd.NaT
        #print(unique_outward_ticker)
        for ind in unique_outward_ticker.index:
            ticker = unique_outward_ticker['Ticker'][ind]
            Dividend_csv = pd.DataFrame(dataframe_csv_outward.loc[dataframe_csv_outward['Ticker'] == ticker, 'Calendar Date'])
            Dividend_csv['Calendar Date'] = pd.to_datetime(Dividend_csv['Calendar Date'])
            Dividend_csv['AssumedDeclarationDate'] = Dividend_csv['Calendar Date'] - timedelta(30)
            Dividend_csv.sort_values('AssumedDeclarationDate')
            #Dividend_csv.drop(['Calendar Date'], axis=1, inplace=True)
            Dividend_csv['DeclarationYear'] =  pd.DatetimeIndex(Dividend_csv['AssumedDeclarationDate']).year
            Dividend_csv['DeclaratioMonth'] =  pd.DatetimeIndex(Dividend_csv['AssumedDeclarationDate']).month
            Dividend_csv['DeclarationDate'] =  pd.DatetimeIndex(Dividend_csv['AssumedDeclarationDate']).day
            #print(Dividend_csv)


            Dividend_bq = pd.DataFrame(dataframe_bq.loc[dataframe_bq['Ticker'] == ticker, 'Dividend'])
            Dividend = []
            RecentDeclarationDate = pd.np.nan
            if(not Dividend_bq.empty):
                #print('here')
                index = Dividend_bq.index
                #print(index[0])
                Dividend = json_normalize(Dividend_bq['Dividend'][index[0]])
                Dividend['DeclarationYear'] = Dividend['DeclarationYear'].apply(int)
                Dividend['DeclaratioMonth'] = Dividend['DeclaratioMonth'].apply(int)
                Dividend['DeclarationDate'] = Dividend['DeclarationDate'].apply(int)
                Dividend_csv_temp = Dividend_csv[['DeclarationYear','DeclaratioMonth', 'DeclarationDate']]
                Dividend = Dividend.append(Dividend_csv_temp)
                Dividend = Dividend.drop_duplicates(subset=['DeclarationYear','DeclaratioMonth', 'DeclarationDate'])
                Dividend = Dividend.sort_values(['DeclarationYear','DeclaratioMonth', 'DeclarationDate'], ascending=False)
                RecentDeclarationDate = datetime(Dividend.iloc[0]['DeclarationYear'],Dividend.iloc[0]['DeclaratioMonth'],Dividend.iloc[0]['DeclarationDate'])
                Dividend['DeclarationYear'] = Dividend['DeclarationYear'].apply(str)
                Dividend['DeclaratioMonth'] = Dividend['DeclaratioMonth'].apply(str)
                Dividend['DeclarationDate'] = Dividend['DeclarationDate'].apply(str)
                dataframe_bq.drop(dataframe_bq.loc[dataframe_bq['Ticker']==ticker].index, inplace=True)
                #print(Dividend)
                #print(RecentDeclarationDate)
            else:
                Dividend_csv_temp = Dividend_csv[['DeclarationYear','DeclaratioMonth', 'DeclarationDate']]
                Dividend = Dividend_csv_temp.drop_duplicates(subset=['DeclarationYear','DeclaratioMonth', 'DeclarationDate'])
                Dividend = Dividend.sort_values(['DeclarationYear','DeclaratioMonth', 'DeclarationDate'], ascending=False)
                RecentDeclarationDate = datetime(Dividend.iloc[0]['DeclarationYear'],Dividend.iloc[0]['DeclaratioMonth'],Dividend.iloc[0]['DeclarationDate'])
                Dividend['DeclarationYear'] = Dividend['DeclarationYear'].apply(str)
                Dividend['DeclaratioMonth'] = Dividend['DeclaratioMonth'].apply(str)
                Dividend['DeclarationDate'] = Dividend['DeclarationDate'].apply(str)
                #print(type(json.loads(Dividend.to_json(orient="records"))))
            #print(Dividend)
            unique_outward_ticker['Dividend'][ind] = json.loads(Dividend.to_json(orient="records"))
            unique_outward_ticker['RecentDeclarationDate'][ind] = RecentDeclarationDate
            unique_outward_ticker['NextPayableDate'][ind] = RecentDeclarationDate + timedelta(30)
            #print(dataframe_csv_outward.loc[dataframe_csv_outward['Ticker'] == ticker])
        #dataframe_csv_outward.drop(['Calendar Date'], axis=1,inplace=True)
        #print(unique_outward_ticker)
        dataframe_bq = dataframe_bq.append(unique_outward_ticker)
        #print(dataframe_bq.loc[dataframe_bq['Ticker'] == 'NXPI'])
        #dataframe_bq.loc[dataframe_bq['Mic'] == outward_Mic, 'Contacts'] = Contacts
        #dataframe_bq = dataframe_bq.head()
        #print((dataframe_bq)) 

    if not filepaths_inward.empty:
        dataframe_csv_inward = pd.concat((pd.read_csv(f) for f in filepaths_inward)) 
        #dataframe_csv_inward = pd.read_csv('gs://csv_triggered_dataflow/Swift_inward.csv')
        dataframe_csv_inward = dataframe_csv_inward[['Ordering Customer line 1 (Tag 50)','Calendar Date']] 
        #print(dataframe_csv_inward.dtypes)
        print(len(dataframe_csv_inward))
        dataframe_csv_inward = dataframe_csv_inward.drop_duplicates(subset=['Ordering Customer line 1 (Tag 50)','Calendar Date'])
        dataframe_csv_inward.rename(columns = {'Ordering Customer line 1 (Tag 50)':'Ticker'}, inplace = True)   
        #print(dataframe_csv_inward.dtypes)
        print(len(dataframe_csv_inward))   
        unique_inward_ticker = pd.DataFrame(dataframe_csv_inward['Ticker'].unique())
        unique_inward_ticker.rename(columns = {0:'Ticker'}, inplace = True)
        unique_inward_ticker['Mic'] = inward_Mic
        unique_inward_ticker['Contacts'] = unique_inward_ticker.apply(lambda x: Contacts, axis=1)
        unique_inward_ticker['Dividend'] = unique_inward_ticker.apply(lambda x: [], axis=1)
        unique_inward_ticker['Period'] = 1
        unique_inward_ticker['RecentDeclarationDate'] = pd.NaT
        unique_inward_ticker['NextPayableDate'] = pd.NaT
        #print(unique_inward_ticker)
        #df_tobq = pd.DataFrame(columns=['Ticker', 'Mic', 'Contacts', 'Dividend', 'RecentDeclarationDate', 'NextPayableDate', 'ExpectedStartDate', 'ExpectedEndDate', 'LastRunDate', 'ProbabilityNextMonthDeclaration', 'Period'])   
        for ind in unique_inward_ticker.index:
            ticker = unique_inward_ticker['Ticker'][ind]
            Dividend_csv = pd.DataFrame(dataframe_csv_inward.loc[dataframe_csv_inward['Ticker'] == ticker, 'Calendar Date'])
            Dividend_csv['Calendar Date'] = pd.to_datetime(Dividend_csv['Calendar Date'])
            Dividend_csv['AssumedDeclarationDate'] = Dividend_csv['Calendar Date'] - timedelta(30)
            Dividend_csv.sort_values('AssumedDeclarationDate')
            #Dividend_csv.drop(['Calendar Date'], axis=1, inplace=True)
            Dividend_csv['DeclarationYear'] =  pd.DatetimeIndex(Dividend_csv['AssumedDeclarationDate']).year
            Dividend_csv['DeclaratioMonth'] =  pd.DatetimeIndex(Dividend_csv['AssumedDeclarationDate']).month
            Dividend_csv['DeclarationDate'] =  pd.DatetimeIndex(Dividend_csv['AssumedDeclarationDate']).day
            #print(Dividend_csv)


            Dividend_bq = pd.DataFrame(dataframe_bq.loc[dataframe_bq['Ticker'] == ticker, 'Dividend'])
            Dividend = []
            RecentDeclarationDate = pd.NaT
            if(not Dividend_bq.empty):
                print('here')
                index = Dividend_bq.index
                Dividend = json_normalize(Dividend_bq['Dividend'][index[0]])
                Dividend['DeclarationYear'] = Dividend['DeclarationYear'].apply(int)
                Dividend['DeclaratioMonth'] = Dividend['DeclaratioMonth'].apply(int)
                Dividend['DeclarationDate'] = Dividend['DeclarationDate'].apply(int)
                Dividend_csv_temp = Dividend_csv[['DeclarationYear','DeclaratioMonth', 'DeclarationDate']]
                Dividend = Dividend.append(Dividend_csv_temp)
                Dividend = Dividend.drop_duplicates(subset=['DeclarationYear','DeclaratioMonth', 'DeclarationDate'])
                Dividend = Dividend.sort_values(['DeclarationYear','DeclaratioMonth', 'DeclarationDate'], ascending=False)
                RecentDeclarationDate = datetime(Dividend.iloc[0]['DeclarationYear'],Dividend.iloc[0]['DeclaratioMonth'],Dividend.iloc[0]['DeclarationDate'])
                Dividend['DeclarationYear'] = Dividend['DeclarationYear'].apply(str)
                Dividend['DeclaratioMonth'] = Dividend['DeclaratioMonth'].apply(str)
                Dividend['DeclarationDate'] = Dividend['DeclarationDate'].apply(str)
                dataframe_bq.drop(dataframe_bq.loc[dataframe_bq['Ticker']==ticker].index, inplace=True)
                #print(Dividend)
            else:
                Dividend_csv_temp = Dividend_csv[['DeclarationYear','DeclaratioMonth', 'DeclarationDate']]
                Dividend = Dividend_csv_temp.drop_duplicates(subset=['DeclarationYear','DeclaratioMonth', 'DeclarationDate'])
                Dividend = Dividend.sort_values(['DeclarationYear','DeclaratioMonth', 'DeclarationDate'], ascending=False)
                RecentDeclarationDate = datetime(Dividend.iloc[0]['DeclarationYear'],Dividend.iloc[0]['DeclaratioMonth'],Dividend.iloc[0]['DeclarationDate'])
                Dividend['DeclarationYear'] = Dividend['DeclarationYear'].apply(str)
                Dividend['DeclaratioMonth'] = Dividend['DeclaratioMonth'].apply(str)
                Dividend['DeclarationDate'] = Dividend['DeclarationDate'].apply(str)
                #print(Dividend)
            unique_inward_ticker['Dividend'][ind] = json.loads(Dividend.to_json(orient="records"))
            unique_inward_ticker['RecentDeclarationDate'][ind] = RecentDeclarationDate
            unique_inward_ticker['NextPayableDate'][ind] = RecentDeclarationDate + timedelta(30)
            #print(dataframe_csv_outward)
        #dataframe_csv_inward.drop(['Calendar Date'], axis=1,inplace=True)
        #print(unique_inward_ticker)

        dataframe_bq = dataframe_bq.append(unique_inward_ticker)
        #print(dataframe_bq)

    dataframe_bq = dataframe_bq.drop_duplicates(subset=['Ticker','Mic'])
    dataframe_bq['NextPayableDate'] = dataframe_bq['NextPayableDate'].dt.strftime('%Y-%m-%d')
    dataframe_bq['ExpectedStartDate'] = dataframe_bq['ExpectedStartDate'].dt.strftime('%Y-%m-%d')
    dataframe_bq['ExpectedEndDate'] = dataframe_bq['ExpectedEndDate'].dt.strftime('%Y-%m-%d')
    dataframe_bq['LastRunDate'] = dataframe_bq['LastRunDate'].dt.strftime('%Y-%m-%d')
    dataframe_bq['RecentDeclarationDate'] = dataframe_bq['RecentDeclarationDate'].dt.strftime('%Y-%m-%d')
    json_data = dataframe_bq.to_json(orient="records")
    json_object = json.loads(json_data)
    job = client.load_table_from_json(json_object, table, job_config = job_config)  # Make an API request.
    #os.remove(cwd+"/temp.pkl")
    job.result()
    if not df_file_outward.empty:
        filenames_outward = df_file_outward['fname']
        for file in filenames_outward:
            source_blob = source_bucket.blob(file)
            print("filename is")
            print(file)
            new_blob = source_bucket.copy_blob(source_blob, destination_bucket, file)
            # delete in old destination
            source_blob.delete()

    if not df_file_inward.empty:
        filenames_inward = df_file_inward['fname'] 
        for file in filenames_inward:
            source_blob = source_bucket.blob(file)
            print("filename is")
            print(file)
            new_blob = source_bucket.copy_blob(source_blob, destination_bucket, file)
            # delete in old destination
            source_blob.delete()
        
        
