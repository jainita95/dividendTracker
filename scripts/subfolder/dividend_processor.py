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
from file_to_gcs import ContentToGoogleCloudStorageOperator
from pandas.api.types import is_datetime64_any_dtype as is_datetime
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from string import Template
from sendgrid.helpers.mail import *
import base64
from pandas.io.json import json_normalize
import math
from google.cloud import storage


def date_to_months(dt_start, dt_end):
    one_day = timedelta(1)
    start_dates = [dt_start]
    end_dates = []
    today = dt_start
    while today <= dt_end:
        #print(today)
        tomorrow = today + one_day
        if tomorrow.month != today.month:
            start_dates.append(tomorrow)
            end_dates.append(today)
        today = tomorrow

    end_dates.append(dt_end)
    return start_dates,end_dates

def weird_division(n, d):
    return n / d if d else 0

def dividend_probability_calculator():  
    credentials=service_account.Credentials.from_service_account_info(
           Variable.get("key",deserialize_json=True))

    destination_bucket_name = 'dividend_declarations_hackathon'
    storage_client = storage.Client()
    destination_bucket = storage_client.bucket(destination_bucket_name)

    project_id = 'hackathon-wpb'
    table_id = 'hackathon-wpb.customer_relations.customer_dividend_malaysia'
    query_string = """
       SELECT * 
       FROM hackathon-wpb.customer_relations.customer_dividend_malaysia"""

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




    dataframe_complete = pdgbq.read_gbq(query = query_string,project_id=project_id)
    dataframe=dataframe_complete
    print(dataframe.dtypes)
    print(len(dataframe))
    base = datetime.today().date()
    start_date = base+timedelta(days=30)
    end_date = base+timedelta(days=70)
    df_companies = pd.DataFrame(dataframe_complete.Ticker.unique())
    df_companies.rename(columns = {0:'Ticker'}, inplace = True) 
    df_companies['ProbabilityNextMonthDeclaration'] = 0.0
    df_companies['ExpectedStartDate']=''
    df_companies['ExpectedEndDate']=''
    convert_dict = {'Ticker': str, 
                    'ProbabilityNextMonthDeclaration': float,
                    'ExpectedStartDate': np.datetime64,
                    'ExpectedEndDate': np.datetime64
                   } 

    df_companies = df_companies.astype(convert_dict)
    for ind in df_companies.index:
         company_name = df_companies['Ticker'][ind]
         #get all records for given ticker
         df_company_temp = pd.DataFrame(dataframe.loc[dataframe['Ticker'] == company_name])
         df_company_temp_list = df_company_temp['Dividend']
         index = df_company_temp_list.index
         try:
             df_company_temp_2=json_normalize(df_company_temp_list[index[0]])
             df = pd.DataFrame({'year': df_company_temp_2['DeclarationYear'],
                                'month': df_company_temp_2['DeclaratioMonth'],
                                'day': df_company_temp_2['DeclarationDate']})
             df_company_temp_2['Date']=pd.to_datetime(df)
             df_company_temp_2.drop_duplicates(subset=['DeclarationYear', 'DeclaratioMonth', 'DeclarationDate'], inplace=True)
         except:
             continue
         total_declarations = len(df_company_temp_2['DeclarationYear'].unique())
         recent_years=[]
         non_recent_years=[]
         count_recent = 0
         number_of_recent_years = 0
         number_of_non_recent_years = 0
         count_non_recent = 0
         base_date_minus_5 = base-timedelta(days=1825)
         for ind2 in df_company_temp_2.index:
                months = dataframe.loc[dataframe['Ticker'] == company_name, 'Period']
                #set start and end dates according to period sepcified else default will be taken
                if(not(math.isnan(months))):
                    start_date = base+timedelta(days=int(months)*30)
                    end_date = start_date+timedelta(days=40)
                #break start and end date range into individual months
                start_dates,end_dates = date_to_months(start_date, end_date)    
                date_temp = df_company_temp_2['Date'][ind2]
                #counts total declarations in recent 5 yrs
                if(base_date_minus_5 <= date_temp < base):
                    recent_years.append(date_temp)
                else:
                    non_recent_years.append(date_temp)
                out_fmt = '%Y-%m-%d'
                #for every month check if previous declaration month/date falls in the range

                for start, end in zip(start_dates,end_dates):
                    year = start.year
                    try:
                        if start.replace(year=year) <= date_temp.replace(year=year) < end.replace(year=year):
                            if(base_date_minus_5 <= date_temp < base):
                                count_recent=count_recent+1
                            elif(date_temp < base_date_minus_5):
                                count_non_recent=count_non_recent+1
                    except:
                        #to handle 29th feb
                        one_day = timedelta(1)
                        date_temp = date_temp - one_day
                        if start.replace(year=year) <= date_temp.replace(year=year) < end.replace(year=year):
                            if(base_date_minus_5 <= date_temp < base):
                                count_recent=count_recent+1
                            elif(date_temp < base_date_minus_5):
                                count_non_recent=count_non_recent+1
         number_of_recent_years=(pd.Series(recent_years)).nunique()
         number_of_non_recent_years=(pd.Series(non_recent_years)).nunique()
         probability = ((2*weird_division(count_recent,number_of_recent_years))+(weird_division(count_non_recent,number_of_non_recent_years)))/3
         df_companies['ProbabilityNextMonthDeclaration'][ind] = round(probability, 3)
         df_companies['ExpectedStartDate'][ind] = np.datetime64(start_date)
         df_companies['ExpectedEndDate'][ind] = np.datetime64(end_date)
         dataframe.loc[dataframe['Ticker'] == company_name, 'ProbabilityNextMonthDeclaration'] = str(probability)
         dataframe.loc[dataframe['Ticker'] == company_name, 'ExpectedStartDate'] = np.datetime64(start_date)
         dataframe.loc[dataframe['Ticker'] == company_name, 'ExpectedEndDate'] = np.datetime64(end_date)
    print(df_companies.dtypes)
    print(len(dataframe))
    dataframe_complete.drop(['ProbabilityNextMonthDeclaration', 'ExpectedStartDate', 'ExpectedEndDate', 'LastRunDate'], axis = 1, inplace = True) 
    df_update = pd.merge(dataframe_complete, df_companies, left_on='Ticker', right_on='Ticker')
    df_update['LastRunDate'] = np.datetime64(base)
    df_update['NextPayableDate'] = df_update['NextPayableDate'].dt.strftime('%Y-%m-%d')
    df_update['ExpectedStartDate'] = df_update['ExpectedStartDate'].dt.strftime('%Y-%m-%d')
    df_update['ExpectedEndDate'] = df_update['ExpectedEndDate'].dt.strftime('%Y-%m-%d')
    df_update['LastRunDate'] = df_update['LastRunDate'].dt.strftime('%Y-%m-%d')
    df_update['RecentDeclarationDate'] = df_update['RecentDeclarationDate'].dt.strftime('%Y-%m-%d')
    json_data = df_update.to_json(orient="records")
    json_object = json.loads(json_data)
    job = client.load_table_from_json(json_object, table, job_config = job_config)  # Make an API request.
    filename = 'customer_dividend_malaysia_probability_update_'+str(datetime.now())+'.json'
    blob = destination_bucket.blob(filename)
    blob.upload_from_string(
    data=json.dumps(json_object),
    content_type='application/json')
    job.result()
    count_mails = 0   #remove for actual code
    for ind3 in df_companies.index:
        company_name = df_companies['Ticker'][ind3]
        print(company_name)
        print(ind3)
        probability = df_companies['ProbabilityNextMonthDeclaration'][ind3]
        expected_start_date = df_update.loc[dataframe['Ticker'] == company_name, 'ExpectedStartDate'].iloc[0]
        expected_end_date = df_update.loc[dataframe['Ticker'] == company_name, 'ExpectedEndDate'].iloc[0]
        if(float(probability) > 0.9 and count_mails < 10):
            df_contacts_temp = pd.DataFrame(dataframe.loc[dataframe['Ticker'] == company_name])
            df_contacts_temp_list = df_company_temp['Contacts']
            index = df_contacts_temp_list.index
            df_contacts=json_normalize(df_contacts_temp_list[index[0]])
            contacts = df_contacts.drop_duplicates(subset=['email'], keep = 'last')
            html_string= None
            with open('/home/airflow/gcs/dags/EmailTemplateUpcomingDividend.html', 'r') as f:
                html_string = f.read()
            html_string=html_string.format(code=company_name,startDate=expected_start_date,endDate=expected_end_date,probability=math.ceil(probability*100))
            name = []
            emails= []
            for ind4 in contacts.index:
                name_contact = contacts['Name'][ind4]
                email = contacts['email'][ind4]
                name.append(name_contact)
                emails.append(To(email))
            message = Mail(
                            from_email='jainita95@outlook.com',
                            to_emails=emails,
                            subject = "Notice: An Upcoming Dividend Declaration cited for "+ company_name,
                            html_content=html_string)
            with open('/home/airflow/gcs/dags/hsbcLogo.png', 'rb') as f:
                data = f.read()
                f.close()
            encoded = base64.b64encode(data).decode()    
            attachment = Attachment()
            attachment.file_content = FileContent(encoded)
            attachment.file_type = FileType('image/png')
            attachment.file_name = FileName('hsbcLogo.png')
            attachment.disposition = Disposition('inline')
            attachment.content_id = ContentId('hsbclogo')
            message.add_attachment(attachment)
            try:
                sg = SendGridAPIClient(Variable.get("sendgridapikey"))
                response = sg.send(message)
                count_mails = count_mails + 1
                #print(response.status_code)
                #print(response.body)
                #print(response.headers)
            except Exception as e:
                print(e.message)