def predict_profile():
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

    warnings.filterwarnings("ignore", category=DeprecationWarning) 
    warnings.filterwarnings("ignore", category=FutureWarning) 
    warnings.filterwarnings("ignore", category=UserWarning) 
    source_bucket = 'hackathon-21-customer-profile-details-latest'
    storage_client = storage.Client()
    source_bucket = storage_client.bucket(source_bucket)
    credentials=service_account.Credentials.from_service_account_info(
               Variable.get("key",deserialize_json=True))
    model_bucket = 'hackathon-21-customer-profile-details-temp'
    storage_client = storage.Client()
    model_bucket = storage_client.bucket(model_bucket)
    
    filepaths_inward = []
    fnames_inward = []
    updated_inward = []
    dict_df = {}
    def credit_score_table(row): 
        credit_score = row.CreditScore
        if credit_score >= 300 and credit_score < 500:
            return "Very_Poor"
        elif credit_score >= 500 and credit_score < 601:
            return "Poor"
        elif credit_score >= 601 and credit_score < 661:
            return "Fair"
        elif credit_score >= 661 and credit_score < 781:
            return "Good"
        elif credit_score >= 851:
            return "Top"
        elif credit_score >= 781 and credit_score < 851:
            return "Excellent"
        elif credit_score < 300:
            return "Deep"
    def product_utilization_rate_by_year(row):
        number_of_products = row.NumOfProducts
        tenure = row.Tenure

        if number_of_products == 0:
            return 0

        if tenure == 0:
            return number_of_products

        rate = number_of_products / tenure
        return rate
    def product_utilization_rate_by_estimated_salary(row):
        number_of_products = row.number_of_products
        estimated_salary = row.EstimatedSalary

        if number_of_products == 0:
            return 0


        rate = number_of_products / estimated_salary
        return rate
    def countries_monthly_average_salaries(row):
        #brutto datas from  https://tr.wikipedia.org/wiki/Aylık_ortalama_ücretlerine_göre_Avrupa_ülkeleri_listesi
        fr = 3696    
        de = 4740
        sp = 2257
        salary = row.EstimatedSalary / 12
        country = row.Geography              # Germany, France and Spain

        if country == 'Germany':
            return salary / de
        elif country == "France":
            return salary / fr
        elif country == "Spain": 
            return salary / sp
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
    pickle_path = os.path.join(os.path.dirname(__file__), "jsonTemp2.pkl")
    pickle_blob = model_bucket.blob("model.pkl").download_to_filename(pickle_path)
    pickle_blob = pickle.load(open(pickle_path,'rb'))
    df_thresholds_path = os.path.join(os.path.dirname(__file__), "threshold.pkl")
    pickle_threshold = model_bucket.blob("threshold.pkl").download_to_filename(df_thresholds_path)
    pickle_threshold = pickle.load(open(df_thresholds_path,'rb'))
    df_threshold = pickle_threshold
    logr_model = pickle_blob
    print("loaded model successfully")
    if len(filepaths_inward) > 0:
        df_input = pd.concat((pd.read_csv(f) for f in filepaths_inward)) 
        df_input = df_input.dropna()
        df_input.head()
        df=df_input[['CustomerId','Surname','CreditScore','Geography','Gender','Age','Tenure','Balance','NumOfProducts','HasCrCard','IsActiveMember','EstimatedSalary','# Error logs','Exited']].copy()

        df_prep = df.copy()
        print(df_prep)
        missing_value_len = df.isnull().any().sum()
        if missing_value_len == 0:
            print("No Missing Value")
        else:
            print("Investigate Missing Value, Missing Value : " + str(missing_value_len))
        print("\n")

        df_prep['Tenure'] =  df_prep.Tenure.astype(np.float)
        df_prep['NumOfProducts'] =  df_prep.NumOfProducts.astype(np.float)

        df_fe = df_prep.copy()
        balance_salary_rate = 'balance_salary_rate'
        df_fe[balance_salary_rate] = df_fe.Balance / df_fe.EstimatedSalary
        df_fe = df_fe.assign(product_utilization_rate_by_year=df_fe.apply(lambda x: product_utilization_rate_by_year(x), axis=1)) 
        tenure_rate_by_age = 'tenure_rate_by_age'
        df_fe[tenure_rate_by_age] = df_fe.Tenure / (df_fe.Age-17)
        credit_score_rate_by_age = 'credit_score_rate_by_age'
        df_fe[credit_score_rate_by_age] = df_fe.CreditScore / (df_fe.Age-17)
        product_utilization_rate_by_salary = 'product_utilization_rate_by_salary'
        credit_score_rate_by_salary = 'credit_score_rate_by_salary'
        df_fe[credit_score_rate_by_salary] = df_fe.CreditScore / (df_fe.EstimatedSalary)
        df_fe = df_fe.assign(credit_score_table=df_fe.apply(lambda x: credit_score_table(x), axis=1))
        df_fe = df_fe.assign(countries_monthly_average_salaries = df_fe.apply(lambda x: countries_monthly_average_salaries(x), axis=1))
        print(df_fe.head(3))

        df_model = df_fe.copy()
        non_encoding_columns = ["Geography","HasCrCard","IsActiveMember","Gender","NumOfProducts","Tenure","credit_score_table","# Error logs"]
        df_non_encoding = df_model[non_encoding_columns]
        df_model = df_model.drop(non_encoding_columns,axis=1)
        df_encoding = df_non_encoding.copy()
        encoder = LabelEncoder()
        df_encoding["gender_category"] = encoder.fit_transform(df_non_encoding.Gender)
        df_encoding["country_category"] = encoder.fit_transform(df_non_encoding.Geography)
        df_encoding["credit_score_category"] = encoder.fit_transform(df_non_encoding.credit_score_table)
        df_encoding.reset_index(drop=True, inplace=True)
        df_model.reset_index(drop=True, inplace=True)
        df_model = pd.concat([df_model,df_encoding],axis=1)
        df_model = df_model.drop(["Geography","Gender","CustomerId","Surname","credit_score_table","CreditScore","EstimatedSalary"],axis=1)
        df_model = df_model.reset_index()
        df_model = df_model.drop('index',axis=1)  
        df_model.loc[df_model.HasCrCard == 0, 'credit_card_situation'] = -1
        df_model.loc[df_model.IsActiveMember == 0, 'is_active_member'] = -1
        df_model.drop(['credit_card_situation', 'is_active_member'], axis=1, inplace=True)
        X_test = df_model.loc[:, df_model.columns != dependent_variable_name]
        y_test = df_model[dependent_variable_name]

        y_pred = logr_model.predict(X_test)
        accuracy = accuracy_score(y_test, y_pred)
        df_input[dependent_variable_name] = y_pred
        df_input['isCreditScoreFactor']=0
        df_input['isAgeFactor']=0
        df_input['isBalanceFactor']=0
        df_input['isNumOfProductsFactor']=0
        df_input['isHasCrCardFactor']=0
        df_input['isActiveMemberFactor']=0
        df_input['isErrorLogsFactor']=0
        for ind in df_input.index:
            isExited = df_input[dependent_variable_name][ind]
            customer_id = df_input['CustomerId'][ind]
            credit_score = df_input['CreditScore'][ind]
            age = df_input['Age'][ind]
            balance = df_input['Balance'][ind]
            no_of_products = df_input['NumOfProducts'][ind]
            has_credit_card = df_input['HasCrCard'][ind]
            is_active_member = df_input['IsActiveMember'][ind]
            no_of_error_logs = df_input['# Error logs'][ind]
            if(isExited==1):
                if(credit_score < df_threshold.loc[df_threshold['contributing_factor'] == 'isCreditScoreFactor', 'max_threshold'].iloc[0]):
                    print(df_input.loc[df_input['CustomerId'] == 'customer_id', 'isCreditScoreFactor'])
                    df_input.loc[df_input['CustomerId'] == 'customer_id', 'isCreditScoreFactor'] = 1
                if((df_threshold.loc[df_threshold['contributing_factor'] == 'isAgeFactor', 'min_threshold'].iloc[0]) <= age <= (df_threshold.loc[df_threshold['contributing_factor'] == 'isAgeFactor', 'max_threshold'].iloc[0])):
                    df_input['isAgeFactor'][ind] = 1
                if((df_threshold.loc[df_threshold['contributing_factor'] == 'isBalanceFactor', 'min_threshold'].iloc[0]) <= balance <= (df_threshold.loc[df_threshold['contributing_factor'] == 'isBalanceFactor', 'max_threshold'].iloc[0])):
                    df_input['isBalanceFactor'][ind] = 1
                if((df_threshold.loc[df_threshold['contributing_factor'] == 'isNumOfProductsFactor', 'min_threshold'].iloc[0]) <= no_of_products <= (df_threshold.loc[df_threshold['contributing_factor'] == 'isNumOfProductsFactor', 'max_threshold'].iloc[0])):
                    df_input['isNumOfProductsFactor'][ind] = 1
                if((df_threshold.loc[df_threshold['contributing_factor'] == 'isHasCrCardFactor', 'min_threshold'].iloc[0]) <= has_credit_card <= (df_threshold.loc[df_threshold['contributing_factor'] == 'isHasCrCardFactor', 'max_threshold'].iloc[0])):
                    df_input['isHasCrCardFactor'][ind] = 1
                if((df_threshold.loc[df_threshold['contributing_factor'] == 'isActiveMemberFactor', 'min_threshold'].iloc[0]) <= is_active_member <= (df_threshold.loc[df_threshold['contributing_factor'] == 'isActiveMemberFactor', 'max_threshold'].iloc[0])):
                    df_input['isActiveMemberFactor'][ind] = 1
                if((df_threshold.loc[df_threshold['contributing_factor'] == 'isErrorLogsFactor', 'min_threshold'].iloc[0]) <= no_of_error_logs <= (df_threshold.loc[df_threshold['contributing_factor'] == 'isErrorLogsFactor', 'max_threshold'].iloc[0])):
                    df_input['isErrorLogsFactor'][ind] = 1
        df_input.rename(columns={"# Error logs": "Errorlogs"}, inplace=True)
        json_data_path = os.path.join(os.path.dirname(__file__), "temp_csv.csv")
        df_input.to_csv(json_data_path)
        model_bucket.blob('temp_csv.csv').upload_from_filename(json_data_path)
        
        #counts-pie
        count_isCreditScoreFactor = len(df_input.loc[df_input['isCreditScoreFactor'] == 1])
        count_isAgeFactor = len(df_input.loc[df_input['isAgeFactor'] == 1])
        count_isBalanceFactor = len(df_input.loc[df_input['isBalanceFactor'] == 1])
        count_isNumOfProductsFactor = len(df_input.loc[df_input['isNumOfProductsFactor'] == 1])
        count_isHasCrCardFactor = len(df_input.loc[df_input['isHasCrCardFactor'] == 1])
        count_isActiveMemberFactor = len(df_input.loc[df_input['isActiveMemberFactor'] == 1])
        count_isErrorLogsFactor = len(df_input.loc[df_input['isErrorLogsFactor'] == 1])
        contributing_factor = ['isCreditScoreFactor','isAgeFactor','isBalanceFactor','isNumOfProductsFactor','isHasCrCardFactor','isActiveMemberFactor','isErrorLogsFactor']
        count = [count_isCreditScoreFactor,count_isAgeFactor,count_isBalanceFactor,count_isNumOfProductsFactor,count_isHasCrCardFactor,count_isActiveMemberFactor,count_isErrorLogsFactor]
        list_of_tuples = list(zip(contributing_factor, count))
        df_contributing_factors = pd.DataFrame(list_of_tuples, 
                      columns = ['Contributing factor', 'Count'])
        json_data_path_contributing_factors = os.path.join(os.path.dirname(__file__), 'counts_pie.csv')
        df_contributing_factors.to_csv(json_data_path_contributing_factors, index=False)
        model_bucket.blob('counts_pie.csv').upload_from_filename(json_data_path_contributing_factors)

        #dailycounts
        file_path_daily_counts = ['gs://hackathon-21-customer-profile-details-temp/day_threshold.csv']
        now = datetime.now()
        dt_string = now.strftime("%m/%d/%Y")
        df_dailycounts = pd.concat((pd.read_csv(f) for f in file_path_daily_counts))
        print(df_dailycounts)
        print(df_dailycounts.dtypes)
        df_dailycounts.loc[len(df_dailycounts.index)] = [dt_string,count_isCreditScoreFactor,count_isAgeFactor,count_isBalanceFactor,count_isNumOfProductsFactor,count_isHasCrCardFactor,count_isActiveMemberFactor,count_isErrorLogsFactor] 
        json_data_path_dailycounts = os.path.join(os.path.dirname(__file__), 'day_threshold.csv.csv')
        df_dailycounts.dropna(inplace=True)
        print(df_dailycounts)
        df_dailycounts.to_csv(json_data_path_dailycounts, index=False)
        model_bucket.blob('day_threshold.csv').upload_from_filename(json_data_path_dailycounts)

