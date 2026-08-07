import pandas as pd
import matplotlib.pyplot as plt
import telegram
import pandahouse as ph
import numpy as np
from datetime import date, timedelta, datetime
import io
import sys
import os
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


class Getch:
    def __init__(self, query, db='simulator'):
        self.connection = {
            'host': 'host',
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
    'host': 'host',
    'database':'simulator_20260120',
    'user':'user', 
    'password':'pass'
}

default_args = {
    'owner': 'e.v.bocharova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2026, 4, 28),
}

schedule_interval = timedelta(minutes=15)

my_token = 'my_token' 
chat_id = 123456789
bot = telegram.Bot(token=my_token) 


def detect_anomaly_iqr(df, metric):
    current_ts = df['ts'].max()
    current_slot = current_ts.hour * 4 + current_ts.minute // 15
    historical_data = df[(df['slot'] == current_slot)]  
    current_value = df[df['ts'] == current_ts][metric].iloc[0]
    values = historical_data[metric].values
    
    q25 = np.percentile(values, 25)
    q75 = np.percentile(values, 75)
    iqr = q75 - q25
    a = 1.5
    if iqr == 0: 
        return 0
    
    lower = np.round(q25 - a * iqr, 2)
    upper = np.round(q75 + a * iqr, 2)
    
    is_anomaly = (current_value < lower) or (current_value > upper)
    
    return {
        'is_anomaly': is_anomaly,
        'current_value': current_value,
        'lower': lower,
        'upper': upper,
        }  


def detect_anomaly_rolling_window(df, metric):
    df = df.sort_values('ts').copy().set_index('ts')
    window = '2h' 
    a = 3

    df['mean'] = df[metric].shift(1).rolling(window=window).mean()
    df['std'] = df[metric].shift(1).rolling(window=window).std()
    df['upper'] = df['mean'] + a * df['std']
    df['lower'] = df['mean'] - a * df['std']
    
    current_value = df[metric].iloc[-1]
    upper = np.round(df['upper'].iloc[-1], 2)
    lower = np.round(df['lower'].iloc[-1], 2)
    is_anomaly = (current_value > upper) or (current_value < lower)

    return {
        'is_anomaly': is_anomaly,
        'current_value': current_value,
        'lower': lower,
        'upper': upper,
        } 


def detect_anomaly_forecasting(df, metric, seasonal_period=96):
    df = df.sort_values('ts').copy()
    df.set_index('ts', inplace=True)
    df = df.asfreq('15T')
    
    series = df[metric].astype(float)
    train = series.iloc[:-1]
    test = series.iloc[-1]
    
    model = ExponentialSmoothing(
        train,
        trend='add',
        seasonal='add',
        seasonal_periods=seasonal_period
    )
    
    model_fit = model.fit()
    forecast = model_fit.forecast(1)
    pred = forecast.iloc[0]
    
    residuals = train - model_fit.fittedvalues
    std = residuals.std()
    lower = np.round(pred - 1.96 * std, 2)
    upper = np.round(pred + 1.96 * std, 2)
    
    is_anomaly = (test < lower) or (test > upper)
    
    return {
        'is_anomaly': is_anomaly,
        'current_value': test,
        'lower': lower,
        'upper': upper,
        } 


def plot_metric(df, metric):
    df = df.sort_values('ts').iloc[-192:].copy()
    
    plt.figure(figsize=(10, 5))
    plt.plot(df['ts'], df[metric], label=metric)
    plt.scatter(df['ts'].iloc[-1], df[metric].iloc[-1], color='red')
    
    plt.title(f"Anomaly detected ({df['ts'].iloc[-1]}): {metric}")
    plt.legend()
    
    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.seek(0)
    plot_object.name = f"{metric}.png"
    plt.close()
    
    return plot_object



@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def telegram_alert_system():
        
    @task 
    def extract_feed_data():
        feed_df = Getch("""
                SELECT
                    toStartOfFifteenMinutes(time) as ts,
                    toHour(time) * 4 + intDiv(toMinute(time), 15) as slot,
                    countIf(action='like') as likes, 
                    countIf(action='view') as views, 
                    countIf(action='like')/countIf(action='view') as ctr, 
                    count(distinct user_id) as dau_feed
                FROM simulator_20260120.feed_actions
                WHERE toDate(time) >= today() - 7 AND ts <= now() - INTERVAL 15 MINUTE
                GROUP BY ts, slot
                """).df
        return feed_df.to_json() 

    @task
    def run_detection(feed_df_json):
        df = pd.read_json(feed_df_json)

        metrics = ['likes', 'views', 'ctr', 'dau_feed']
        methods = {
            'iqr': detect_anomaly_iqr,
            'rolling_window': detect_anomaly_rolling_window,
            'forecasting': detect_anomaly_forecasting
        }

        results = {name: {} for name in methods}

        for metric in metrics:
            for method_name, func in methods.items():
                try:
                    func_results = func(df, metric)
                    results[method_name][metric] = {
                        'is_anomaly': func_results['is_anomaly'],
                        'value': func_results['current_value'],
                        'lower': func_results['lower'],
                        'upper': func_results['upper']
                    }
                except Exception as e:
                    results[method_name][metric] = {
                        'is_anomaly': None,
                        'error': str(e)
                    }

        return results
    
    @task
    def aggregate_results(results):
        metrics = ['likes', 'views', 'ctr', 'dau_feed']
        alert_metrics = []

        for metric in metrics:
            votes = 0
            valid_methods = 0

            for method_name in results:
                res = results[method_name][metric]

                if res['is_anomaly'] is not None:
                    valid_methods += 1
                    if res['is_anomaly']:
                        votes += 1

            if votes >= 2:
                alert_metrics.append(metric)

        return alert_metrics
    
    @task
    def send_alerts(feed_df_json, results, alert_metrics):
        df = pd.read_json(feed_df_json)

        if not alert_metrics:
            return

        for metric in alert_metrics:
            img = plot_metric(df, metric)
            bot.sendPhoto(chat_id=chat_id, photo=img)

        alert_messages = []
        for metric in alert_metrics:
            for method_name in results:
                res = results[method_name][metric]

                if res['is_anomaly']:
                    text = (
                        f"Метрика: {metric}\n"
                        f"Метод: {method_name}\n"
                        f"Текущее значение: {res['value']}\n"
                        f"Границы: от {res['lower']} до {res['upper']}"
                    )
                    alert_messages.append(text)
                    break

        bot.sendMessage(chat_id=chat_id, text="\n\n".join(alert_messages))

        
    df = extract_feed_data()
    results = run_detection(df)
    alerts = aggregate_results(results)
    send_alerts(df, results, alerts)

    
alert_dag = telegram_alert_system()  