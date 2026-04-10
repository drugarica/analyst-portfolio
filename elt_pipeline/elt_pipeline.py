from datetime import datetime, timedelta
import pandas as pd
import pandahouse as ph
import numpy as np
from io import StringIO
import requests

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


def ch_get_df(query='Select 1', host='host', user='user', password='pass'):
    r = requests.post(host, data=query.encode("utf-8"), auth=(user, password), verify=False)
    result = pd.read_csv(StringIO(r.text), sep='\t')
    return result

connection_test = {'host': 'host',
                    'database':'test',
                    'user':'user', 
                    'password':'pass'
                  }

query_test = '''CREATE TABLE IF NOT EXISTS test.bocharova_e_v
                    (event_date Date,
                     dimension String,
                     dimension_value String,
                     views Int32,
                     likes Int32,
                     messages_received Int32,
                     messages_sent Int32,
                     users_received Int32,
                     users_sent Int32
                    )
                ENGINE = MergeTree()
                ORDER BY event_date
            '''

ph.execute(query_test, connection=connection_test)

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'e.v.bocharova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 12, 7),
}

# Интервал запуска DAG
schedule_interval = '0 1 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=True)
def get_info():

    @task()
    def extract_actions():
        query_a = """
            SELECT user_id AS user, countIf(action='like') AS likes, countIf(action='view') AS views, 
                   toDate(time) AS event_date, gender, os, age
            FROM simulator_20260120.feed_actions 
            WHERE toDate(time) = today()
            GROUP BY user_id, event_date, gender, os, age
            format TSVWithNames
            """
        df_actions = ch_get_df(query=query_a)
        return df_actions
    
    @task
    def extract_messages():
        query_m = """
            SELECT t1.user AS user, messages_sent, users_sent, messages_received, users_received, 
                   COALESCE(t1.event_date, t2.event_date) AS event_date
            FROM (SELECT user_id AS user, toDate(time) AS event_date, 
                  COUNT(*) AS messages_sent, COUNT(DISTINCT receiver_id) AS users_sent 
                  FROM simulator_20260120.message_actions
                  WHERE toDate(time) < today()
                  GROUP BY user_id, event_date) t1
            FULL JOIN (SELECT receiver_id AS user, toDate(time) AS event_date, 
                       COUNT(*) AS messages_received, COUNT(DISTINCT user_id) AS users_received 
                       FROM simulator_20260120.message_actions
                       WHERE toDate(time) < today()
                       GROUP BY receiver_id, event_date) t2
            ON t1.user = t2.user AND t1.event_date = t2.event_date
            format TSVWithNames
            """
        df_messages = ch_get_df(query=query_m)
        return df_messages
    
    @task 
    def full_merge_data(df1, df2):
        merged_df = df1.merge(df2, on=['user', 'event_date'], how='outer')
        return merged_df 
    
    @task 
    def gender_dimension(df):
        df['gender'] = df['gender'].map({0: 'Man', 1: 'Woman'})
        df['gender'] = df['gender'].fillna('Unknown')
        gender_df = df.groupby(['gender', 'event_date']) \
            .agg({'views': 'sum',
                'likes': 'sum',
                'messages_received': 'sum',
                'messages_sent': 'sum',
                'users_received': 'sum',
                'users_sent': 'sum'
               }).reset_index()
        return gender_df
    
    @task 
    def os_dimension(df): 
        df['os'] = df['os'].fillna('Unknown')
        os_df = df.groupby(['os', 'event_date']) \
        .agg({'views': 'sum',
            'likes': 'sum',
            'messages_received': 'sum',
            'messages_sent': 'sum',
            'users_received': 'sum',
            'users_sent': 'sum'
           }).reset_index()
        return os_df
    
    @task
    def age_dimension(df):
        df['age'] = df['age'].fillna('Unknown')
        df['age'] = df['age'].astype(str)
        age_df = df.groupby(['age', 'event_date']) \
            .agg({'views': 'sum',
                'likes': 'sum',
                'messages_received': 'sum',
                'messages_sent': 'sum',
                'users_received': 'sum',
                'users_sent': 'sum'
               }).reset_index()
        return age_df
    
    @task
    def reshape_table(df, dim_col):
        df_reshaped = df.copy()
        df_reshaped["dimension_value"] = df_reshaped[dim_col].astype(str)
        df_reshaped["dimension"] = dim_col
        cols_order = ["event_date", "dimension", "dimension_value",
                      "views", "likes", "messages_received",
                      "messages_sent", "users_received", "users_sent"]
        return df_reshaped[cols_order]
    
    @task
    def merge_dfs(df_gender, df_os, df_age):
        df_final = pd.concat([df_gender, df_os, df_age], ignore_index=True)
        return df_final
    
    @task
    def process_final(df_final):
        df_final['event_date'] = pd.to_datetime(df_final['event_date'])
        df_final['dimension_value'] = df_final['dimension_value'].astype(str)
        df_final['dimension'] = df_final['dimension'].astype(str)
        df_final['views'] = df_final['views'].fillna(0).astype(np.int32)
        df_final['likes'] = df_final['likes'].fillna(0).astype(np.int32)
        df_final['messages_received'] = df_final['messages_received'].fillna(0).astype(np.int32)
        df_final['messages_sent'] = df_final['messages_sent'].fillna(0).astype(np.int32)
        df_final['users_received'] = df_final['users_received'].fillna(0).astype(np.int32)
        df_final['users_sent'] = df_final['users_sent'].fillna(0).astype(np.int32)
        connection_test = {'host': 'host',
                            'database':'test',
                            'user':'user', 
                            'password':'pass'}
        ph.to_clickhouse(df=df_final, table='bocharova_e_v', connection=connection_test, index=False)

    df_actions = extract_actions()
    df_messages = extract_messages()
    data = full_merge_data(df_actions, df_messages)
    gender_data = gender_dimension(data)
    os_data = os_dimension(data)
    age_data = age_dimension(data)
    df_gender_long = reshape_table(gender_data, "gender")
    df_os_long = reshape_table(os_data, "os")
    df_age_long = reshape_table(age_data, "age")
    df_final = merge_dfs(df_gender_long, df_os_long, df_age_long)
    df_final_processed = process_final(df_final)
    
    
dag = get_info()