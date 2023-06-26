import pandas as pd
import numpy as np
import os
from env import get_db_url
from sklearn.model_selection import train_test_split

def acquire_telco_data():
    """
    This function acquires data from the telco_churn database using a SQL query and returns a pandas dataframe.
    """
    sql = '''
            select *, contract_types.contract_type, internet_service_types.internet_service_type, payment_types.payment_type
            FROM customers
            join contract_types using(contract_type_id)
            join internet_service_types using (internet_service_id)
            join payment_types using(payment_type_id)
          '''
    df = pd.read_sql(sql, get_db_url('telco_churn'))
    return df

def prepare_telco_data(df):
    """
    This function prepares the acquired data by dropping unnecessary columns, filling missing values, and encoding categorical variables.
    """
    df = df.drop(['customer_id', 'gender', 'partner', 'dependents', 'phone_service', 'multiple_lines', 'online_security', 'online_backup', 'device_protection', 'tech_support', 'streaming_tv', 'streaming_movies', 'paperless_billing', 'payment_type_id', 'total_charges'], axis=1)
    df['senior_citizen'] = df['senior_citizen'].replace({0: 'No', 1: 'Yes'})
    df['churn'] = df['churn'].replace({0: 'No', 1: 'Yes'})
    df['internet_service_type_DSL'] = (df['internet_service_type'] == 'DSL').astype(int)
    return df

def split_telco_data(df):
    """
    This function splits the prepared data into training and testing sets and returns them.
    """
    X = df.drop('churn', axis=1)
    y = df['churn']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    return X_train, X_test, y_train, y_test

def get_telco_data():
    """
    This function checks if a CSV file exists with the acquired data. If it does, it reads the file and returns a pandas dataframe. If it doesn't, it acquires the data, saves it to a CSV file, and returns a pandas dataframe.
    """
    if os.path.isfile('telco.csv'):
        df = pd.read_csv('telco.csv', index_col=0)
    else:
        df = acquire_telco_data()
        df.to_csv('telco.csv')
    return df

def customers_with_dsl(df):
    """
    This function returns a pandas dataframe with information about customers with DSL and their churn rate.
    """
    dsl_df = df[df['internet_service_type_DSL'] == 1]
    churn_rate = dsl_df['churn'].value_counts(normalize=True)
    return churn_rate

def month_with_highest_churn(df):
    """
    This function returns the month with the highest churn rate and the corresponding churn rate.
    """
    df['month'] = pd.to_datetime(df['contract_begin_date']).dt.month
    churn_rate = df.groupby('month')['churn'].mean().sort_values(ascending=False)
    return churn_rate.head(1)

def contract_type_and_churn(df):
    """
    This function returns a pandas dataframe with information about contract types and their corresponding churn rate.
    """
    contract_df = df[['contract_type_Month-to-month', 'contract_type_One year', 'contract_type_Two year', 'churn']]
    churn_rate = contract_df.groupby(['contract_type_Month-to-month', 'contract_type_One year', 'contract_type_Two year'])['churn'].mean().sort_values(ascending=False)
    return churn_rate

def service_and_churn(df):
    """
    This function returns a pandas dataframe with information about services and their corresponding churn rate.
    """
    service_df = df[['online_security_Yes', 'online_backup_Yes', 'device_protection_Yes', 'tech_support_Yes', 'streaming_tv_Yes', 'streaming_movies_Yes', 'churn']]
    churn_rate = service_df.groupby(['online_security_Yes', 'online_backup_Yes', 'device_protection_Yes', 'tech_support_Yes', 'streaming_tv_Yes', 'streaming_movies_Yes'])['churn'].mean().sort_values(ascending=False)
    return churn_rate

def avg_monthly_spend(df):
    """
    This function returns the average monthly spend for customers who churned and those who didn't.
    """
    avg_spend_df = df[['monthly_charges', 'churn']]
    churned_df = avg_spend_df[avg_spend_df['churn'] == 'Yes']
    not_churned_df = avg_spend_df[avg_spend_df['churn'] == 'No']
    avg_churned_spend = churned_df['monthly_charges'].mean()
    avg_not_churned_spend = not_churned_df['monthly_charges'].mean()
    return avg_churned_spend, avg_not_churned_spend
