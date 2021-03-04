def train_model_and_store():
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


    credentials=service_account.Credentials.from_service_account_info(
           Variable.get("key",deserialize_json=True))

    warnings.filterwarnings("ignore", category=DeprecationWarning) 
    warnings.filterwarnings("ignore", category=FutureWarning) 
    warnings.filterwarnings("ignore", category=UserWarning) 
    source_bucket = 'hackathon-21-customer-profile-details-historic-data'
    storage_client = storage.Client()
    source_bucket = storage_client.bucket(source_bucket)
    model_bucket = 'hackathon-21-customer-profile-details-temp'
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
    if len(filepaths_inward) > 0:
        df_input = pd.concat((pd.read_csv(f) for f in filepaths_inward)) 
        df_input.head()
        df=df_input[['CustomerId','Surname','CreditScore','Geography','Gender','Age','Tenure','Balance','NumOfProducts','HasCrCard','IsActiveMember','EstimatedSalary','# Error logs','Exited']].copy()

        #dataprep
        df_prep = df.copy()
        missing_value_len = df.isnull().any().sum()
        if missing_value_len == 0:
            print("No Missing Value")
        else:
            print("Investigate Missing Value, Missing Value : " + str(missing_value_len))
        print("\n")

        df_prep['Tenure'] =  df_prep.Tenure.astype(np.float)
        df_prep['NumOfProducts'] =  df_prep.NumOfProducts.astype(np.float)
        numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
        df_num_cols = df_prep.select_dtypes(include=numerics)
        df_outlier = df_num_cols.astype("float64")
        clf = LocalOutlierFactor(n_neighbors = 20, contamination = 0.1)
        clf.fit_predict(df_outlier)
        df_scores = clf.negative_outlier_factor_
        scores_df = pd.DataFrame(np.sort(df_scores))

        #scores_df.plot(stacked=True, xlim = [0,20], color='r', title='Visualization of outliers according to the LOF method', style = '.-');                # first 20 observe
        th_val = np.sort(df_scores)[2]
        outliers = df_scores > th_val
        df_prep = df_prep.drop(df_outlier[~outliers].index)
        df_prep.shape
        Q1 = df["Age"].quantile(0.25)
        Q3 = df["Age"].quantile(0.75)
        IQR = Q3 - Q1
        lower = Q1 - 1.5 * IQR
        upper = Q3 + 1.5 * IQR
        #print("When age and credit score is printed below lower score: ", lower, "and upper score: ", upper)
        df_outlier = df_prep["Age"][(df_prep["Age"] > upper)]
        df_prep["Age"][df_outlier.index] = upper    
        Q1 = df["CreditScore"].quantile(0.25)
        Q3 = df["CreditScore"].quantile(0.75)
        IQR = Q3 - Q1
        lower = Q1 - 1.5 * IQR
        upper = Q3 + 1.5 * IQR
        #print("When age and credit score is printed above lower score: ", lower, "and upper score: ", upper)
        df_outlier = df_prep["CreditScore"][(df_prep["CreditScore"] < lower)]
        df_prep["CreditScore"][df_outlier.index] = lower

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
        #print(df_fe.head(3))

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
        #print(df_model.head(3))

        y = df_model[dependent_variable_name]
        X = df_model.loc[:, df_model.columns != dependent_variable_name]
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.2, random_state = 12345)
        sc = StandardScaler()
        X_train = sc.fit_transform(X_train)
        X_test = sc.transform (X_test)
        logr_model = RandomForestClassifier().fit(X_train,y_train)
        y_pred = logr_model.predict(X_test)
        accuracy = accuracy_score(y_test, y_pred)

        #print(classification_report(y_test, y_pred, digits=4))
        #print("Accuracy score of Logistic Regression: ", accuracy)

        json_data_path = os.path.join(os.path.dirname(__file__), 'jsonTemp.pkl')
        with open(json_data_path, 'wb') as f:
            pickle.dump(logr_model,f)
        model_bucket.blob('model.pkl').upload_from_filename(json_data_path)
        contributing_factor = ['isCreditScoreFactor','isAgeFactor','isBalanceFactor','isNumOfProductsFactor','isHasCrCardFactor','isActiveMemberFactor','isErrorLogsFactor']
        min_threshold = [0,40,50000,3,0,0,6]
        max_threshold = [450,65,100000,4,0,0,10]
        list_of_tuples = list(zip(contributing_factor, min_threshold,max_threshold))
        df_thresholds = pd.DataFrame(list_of_tuples, 
                      columns = ['contributing_factor', 'min_threshold','max_threshold'])
        df_thresholds_path = os.path.join(os.path.dirname(__file__), "threshold.pkl")
        with open(df_thresholds_path, 'wb')as f:
            pickle.dump(df_thresholds,f)
        model_bucket.blob('threshold.pkl').upload_from_filename(df_thresholds_path)
