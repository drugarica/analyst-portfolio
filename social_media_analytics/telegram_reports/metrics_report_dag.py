import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
import pandahouse as ph
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

class Getch:
    def __init__(self, query, db='simulator'):
        self.connection = {
            'host': 'host_address',
            'password': 'pass',
            'user': 'user',
            'database': db,
        }
        self.query = query
        self.getchdf

    @property
    def getchdf(self):
        try:
            self.df = ph.read_clickhouse(self.query, connection=self.connection)

        except Exception as err:
            print("\033[31m {}".format(err))
            exit(0)

connection = {
    'host': 'host_address',
    'database':'simulator_20260120',
    'user':'user', 
    'password':'password'
}

default_args = {
    'owner': 'e.v.bocharova',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2026, 3, 7),
}

# Интервал запуска DAG
schedule_interval = '0 11 * * *'


my_token = 'my_token123' 
chat_id = '12345'
bot = telegram.Bot(token=my_token) 


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def telegram_report_metrics():
    
    @task
    def extract_yesterday(): 
        df_yesterday = Getch("""
                    select today() - 1 AS yesterday, 
                    count(distinct user_id) AS dau, 
                    countIf(action='view') AS views, 
                    countIf(action='like') AS likes, 
                    countIf(action='like')/countIf(action='view') AS CTR
                    from simulator_20260120.feed_actions 
                    where toDate(time) = today() - 1
                    """).df
        return df_yesterday 
    
    @task
    def sending_message(df, chat_id, bot):
        yesterday = pd.to_datetime(df['yesterday'].iloc[0]).date()
        dau = df['dau'].iloc[0] 
        views = df['views'].iloc[0] 
        likes = df['likes'].iloc[0] 
        ctr = np.round(df['CTR'].iloc[0], 2) 
        text_metrics = f"Основные метрики ленты за вчерашний день ({yesterday}): \nDAU: {dau} \nПросмотры: {views} \nЛайки: {likes} \nCTR {ctr}"
        bot.sendMessage(chat_id=chat_id, text=text_metrics)

    @task
    def extract_lastweek():
        df_lastweek = Getch("""
                    select toDate(time) as date, count(distinct user_id) AS dau, countIf(action='view') AS views, 
                    countIf(action='like') AS likes, countIf(action='like')/countIf(action='view') AS CTR
                    from simulator_20260120.feed_actions 
                    where toDate(time) >= today() - 7 and toDate(time) <= today() - 1
                    group by toDate(time)
                    """).df
        return df_lastweek 
    
    @task
    def sending_plots(df, chat_id, bot):
        plt.figure(figsize=[9, 15])

        x = df['date']
        y_dau = df['dau']
        y_views = df['views']
        y_likes = df['likes']
        y_ctr = df['CTR']

        plt.suptitle('Основные метрики за последние 7 дней', fontsize=18, fontweight='600')

        plt.subplots_adjust(top=0.92, bottom=0.08, hspace=0.35)

        plt.subplot(4, 1, 1)
        plt.plot(x, y_dau, 'g')
        plt.title('DAU: Число уникальных пользователей', fontsize=15)

        plt.subplot(4, 1, 2)
        plt.plot(x, y_views, 'r')
        plt.title('Количество просмотров', fontsize=15)

        plt.subplot(4, 1, 3)
        plt.plot(x, y_likes, 'b')
        plt.title('Количество лайков', fontsize=15)

        plt.subplot(4, 1, 4)
        plt.plot(x, y_ctr, 'c')
        plt.title('CTR: коэффициент кликабельности', fontsize=15)

        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'metrics.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        
        
    df_yesterday = extract_yesterday()
    sending_message(df_yesterday, chat_id, bot)
    df_lastweek = extract_lastweek()
    sending_plots(df_lastweek, chat_id, bot)
        
        
yesterday_metrics = telegram_report_metrics()