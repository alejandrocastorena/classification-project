import pandas as pd
import numpy as np
import os
from env import host, user, password

def get_db_url(db, user=user, host=host, password=password):
    return f'mysql+pymysql://{user}:{password}@{host}/{db}'
    
def telco_data():
    sql = '''
            select * FROM customers
            join contract_types using(contract_type_id)
            join internet_service_types using (internet_service_id)
            join payment_types using(payment_type_id)
          '''
    df = pd.read_sql(sql, get_db_url('telco_churn'))
    return df

def get_telco():
    if os.path.isfile('telco.csv'):
        df = pd.read_csv('telco.csv', index_col=0)
        
    else:
        df = new_telco_data()
        df.to_csv('telco.csv')
        
    return df

