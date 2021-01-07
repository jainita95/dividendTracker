def call_dividend_api():
    from airflow.operators.python_operator import PythonOperator
    from airflow.operators.bash_operator  import BashOperator
    from airflow.hooks.base_hook import BaseHook
    from airflow.models import DAG
    from airflow.utils import dates
    from datetime import datetime, timedelta
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
    import time
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
    import pickle


    credentials=service_account.Credentials.from_service_account_info(
           Variable.get("key",deserialize_json=True))

    project_id = 'hackathon-wpb'
    table_id = 'hackathon-wpb.customer_relations.customer_dividend_malaysia'
    query_string = """
       SELECT * 
       FROM hackathon-wpb.customer_relations.customer_dividend_malaysia"""

    url = "https://globalhistorical.xignite.com/v3/xGlobalHistorical.json/GetCashDividendHistory"
    client = bigquery.Client(credentials= credentials,project=project_id)

    job_config = bigquery.LoadJobConfig(
        source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema = [
            bigquery.schema.SchemaField('Ticker', 'STRING', mode='REQUIRED'),
            bigquery.schema.SchemaField('Mic', 'STRING', mode='REQUIRED'),        
            bigquery.schema.SchemaField('Contacts', 'RECORD', mode='REPEATED', fields = [
                bigquery.schema.SchemaField('Name', 'STRING', mode='NULLABLE'),
                bigquery.schema.SchemaField('email', 'STRING', mode='NULLABLE')]),
            bigquery.schema.SchemaField('Dividend', 'RECORD', mode='REPEATED', fields = [
                bigquery.schema.SchemaField('DeclarationYear', 'STRING', mode='NULLABLE'),
                bigquery.schema.SchemaField('DeclaratioMonth', 'STRING', mode='NULLABLE'),
                bigquery.schema.SchemaField('DeclarationDate', 'STRING', mode='NULLABLE')]),        
            bigquery.schema.SchemaField('RecentDeclarationDate', 'DATE', mode='NULLABLE'),
            bigquery.schema.SchemaField('NextPayableDate', 'DATE', mode='NULLABLE'),
            bigquery.schema.SchemaField('ProbabilityNextMonthDeclaration', 'NUMERIC', mode='NULLABLE'),
            bigquery.schema.SchemaField('Period', 'INTEGER', mode='NULLABLE'),
            bigquery.schema.SchemaField('ExpectedStartDate', 'DATE', mode='NULLABLE'),
            bigquery.schema.SchemaField('ExpectedEndDate', 'DATE', mode='NULLABLE'),
            bigquery.schema.SchemaField('LastRunDate', 'DATE', mode='NULLABLE')
        ],
        write_disposition="WRITE_TRUNCATE",
    )
    s = requests.Session()

    retries = Retry(total=5,
                    backoff_factor=0.1,
                    status_forcelist=[500, 502, 503, 504 ,400],
                    raise_on_status =True)

    s.mount('https://', HTTPAdapter(max_retries=retries))

    dataframe = pdgbq.read_gbq(query = query_string,project_id=project_id)
    dataframe['NextPayableDate'] = [d.strftime('%Y-%m-%d') if not pd.isnull(d) else None for d in dataframe['NextPayableDate']]
    dataframe['ExpectedStartDate'] = [d.strftime('%Y-%m-%d') if not pd.isnull(d) else None for d in dataframe['ExpectedStartDate']]
    dataframe['ExpectedEndDate'] = [d.strftime('%Y-%m-%d') if not pd.isnull(d) else None for d in dataframe['ExpectedEndDate']]
    dataframe['LastRunDate'] = [d.strftime('%Y-%m-%d') if not pd.isnull(d) else None for d in dataframe['LastRunDate']]
    dataframe['RecentDeclarationDate'] = [d.strftime('%Y-%m-%d') if not pd.isnull(d) else None for d in dataframe['RecentDeclarationDate']]
    dataframe['Period'] = dataframe['Period'].astype('Int64')

    for ind in dataframe.index: 

          mic = dataframe['Mic'][ind]
          if not (mic == 'HSBC_non_local_customer' or mic == 'HSBC_local_customer'):
              declarationDate = dataframe['RecentDeclarationDate'][ind]
              params = {'IdentifierType' : 'Symbol',
                        'Identifier' :  dataframe.iloc[ind]['Ticker'],
                        'IdentifierAsOfDate': ' ',
                        'StartDate' : '01/01/2018',
                        'EndDate' : datetime.now().strftime('%m/%d/%Y'),
                        'CorporateActionsAdjusted' : 'True',
                        '_token' : Variable.get("xignitetoken")}
              response = s.get(url = url, params = params)
              results =  json.loads(response.text)['CashDividends']
              if len(results) > 0:
                  declaredDate = results[0]['DeclaredDate']
                  if results[0]['DeclaredDate'] == None and results[0]['ExDate'] != None :
                        declaredDate = (datetime.strptime(results[0]['ExDate'],'%Y-%m-%d') - timedelta(30)).strftime('%Y-%m-%d')

                  for i in range(0, len(results)):
                      if not(results[i]['DeclaredDate'] == None and results[i]['ExDate'] ==None) :  

                          if results[i]['DeclaredDate'] == None:
                              dt = datetime.strptime(results[i]['ExDate'],'%Y-%m-%d') - timedelta(30)
                          else:
                              dt = datetime.strptime(results[i]['DeclaredDate'],'%Y-%m-%d')
                          year= dt.year
                          month = dt.month
                          day = dt.day
                          if pd.isnull(declarationDate) :
                              baseArray = dataframe.iloc[ind]['Dividend']
                              if not isinstance(dataframe.iloc[ind]['Dividend'],(np.ndarray)):
                                    baseArray = []
                              newDeclaration = {'DeclarationYear': str(year), 'DeclaratioMonth': str(month), 'DeclarationDate': str(day)}
                              appendedDeclaration=np.append(baseArray,[newDeclaration])
                              dataframe.at[ind,'Dividend']=appendedDeclaration  
                              #query_job.result() 
                          elif dt > datetime.strptime(declarationDate,'%Y-%m-%d'):
                              newDeclaration = {'DeclarationYear': str(year), 'DeclaratioMonth': str(month), 'DeclarationDate': str(day)}
                              appendedDeclaration=np.append(dataframe.iloc[ind]['Dividend'],[newDeclaration])
                              dataframe.at[ind,'Dividend']=appendedDeclaration 
                              #query_job.result() 
                  if pd.isnull(declarationDate) or  (datetime.strptime(declarationDate,'%Y-%m-%d') < datetime.strptime(declaredDate,'%Y-%m-%d')):
                      dataframe.at[ind,'RecentDeclarationDate']=declaredDate
                      dataframe.at[ind,'NextPayableDate']=results[0]['PayDate'] 
                      #query_job.result() 

                  today = date.today()
                  today_datetime = datetime(
                                   year=today.year, 
                                   month=today.month,
                                   day=today.day,
                                )
                  if (pd.isnull(declarationDate) or  (datetime.strptime(declarationDate,'%Y-%m-%d') < datetime.strptime(declaredDate,'%Y-%m-%d'))) and ((datetime.strptime(results[0]['PayDate'],'%Y-%m-%d')+timedelta(30))  > today_datetime):
                        contacts =  dataframe["Contacts"][ind] 

                        html_string= None
                        with open('/home/airflow/gcs/dags/EmailTemplateDividendDeclared.html', 'r') as f:
                            html_string = f.read()
                        html_string=html_string.format(code=dataframe.iloc[ind]['Ticker'],date=results[0]['PayDate'])   
                        name = []
                        emails= []
                        for i in range(0,contacts.size):
                            name.append(contacts[i]['Name'])
                            emails.append(To(contacts[i]['email']))
                        message = Mail(
                                from_email='jainita95@outlook.com',
                                to_emails=emails,
                                subject = "Urgent ! New Dividend Declared for "+ dataframe.iloc[ind]['Ticker'],
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
                        except Exception as e:
                            print(e.message)

    json_df= dataframe.to_json(orient="records")
    json_data = json.loads(json_df)
    job = client.load_table_from_json(json_data , table_id, job_config=job_config)  # Make an API request.
    job.result() 
