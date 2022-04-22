from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from DBHelper import DBHelper
import boto3
import pickle
import pandas as pd
from statsmodels.tsa.arima.model import ARIMA

from S3Helper import get_s3

########################################### CONFIG ###############################################

s3 = get_s3()

########################################### DB HELPER ###############################################

DATABASE_NAME = "company-short-term-analysis"
COMPANY_STREAMING_QUOTES = 'company-streaming-quotes'
COMPANY_COMBINED_QUOTES = 'company-combined-quotes'
COMPANY_SHORT_TERM_TRENDS = "company-short-term-trends"

########################################### STATIC FUNCTIONS ###############################################

def get_data(collection_name):
    db = DBHelper()
    data = db.get_documents_for_collection(DATABASE_NAME, collection_name)
    
    return data

def upload_data(collection_name, collection):
    db = DBHelper()
    db.insert_many_for_collection(DATABASE_NAME, collection_name, collection)

def get_company_predictions(models: dict, data: dict):
    df = pd.DataFrame.from_dict(data)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df.set_index('timestamp', inplace = True)
    df.index = pd.DatetimeIndex(df.index).to_period('min')
    df.sort_index()

    predictions = {}
    new_models = {}
    for col in df.columns:
        if col == 'timestamp':
            continue
        df[col] = pd.to_numeric(df[col])

        model = models[col] if col in model else None
        prediction, new_model = get_LSTM_predictions(df, col, model)
        predictions[col] = prediction
        new_models[col] = new_model

    return predictions, new_models

def get_LSTM_predictions(df: pd.DataFrame, col: str, model):
    if model is None:
        model = ARIMA(df[col], order=(1,1,1))
    else:
        model = model.append(df[col])

    fit = model.fit()

    return fit.forecast(steps = 1).iloc[0], fit

def get_trend(predictions):
    if predictions['bidSize'] == predictions['askSize']:
        return 'flat'

    return 'up' if predictions['bidSize'] > predictions['askSize'] else 'down'

########################################### DAG FUNCTIONS ###############################################

def combine_data(**kwargs):
    db_data = get_data(COMPANY_STREAMING_QUOTES)
    
    # Data should be in the form
    # [{'company': 'abc', 'timestamp': DateTime, 'askSize': '1', 'bidSize': '1',}...]

    combined_data = {}
    for data in db_data:
        if data['company'] not in combined_data:
            combined_data[data['company']] = {}

        for key in data.keys():
            if key == 'company':
                continue

            if key in combined_data[data['company']]:
                combined_data[data['company']][key].append(data[key])
            else:
                combined_data[data['company']][key] = [data[key]]

    upload_data(COMPANY_COMBINED_QUOTES, combined_data)

def get_models():
    try:
        s3_models = s3.Object('is3107-models', 'short-term-arima.pkl').get()
        models = pickle.load(s3_models['Body'])
        return models
    except:
        return None

def generate_predictions(**kwargs):
    combined_data = get_data(COMPANY_COMBINED_QUOTES)
    models = get_models()

    trends = {}
    new_models = {}
    for company, data in combined_data.items():
        model = models[company] if company in models else {}
        predictions, new_model = get_company_predictions(model, data) # should be a dictionary of next 5 values of each item
        trends[company] = get_trend(predictions)
        new_models[company] = new_model

    kwargs['ti'].xcom_push(key='trends', value=trends)
    kwargs['ti'].xcom_push(key='new_models', value=new_models)

    db = DBHelper()
    db.insert_many_for_collection(DATABASE_NAME, COMPANY_SHORT_TERM_TRENDS, trends)

def save_model(**kwargs):
    models = kwargs['ti'].xcom_pull(key='new_models', task_ids='generate_predictions')
    serialized_models = pickle.dumps(models)
    s3.Object('is3107-models', 'short-term-arima.pkl').put(Body=serialized_models)

def push_to_db(**kwargs):
    db = DBHelper()
    db.export_collection_to_json(DATABASE_NAME, COMPANY_SHORT_TERM_TRENDS)

def clean_db(**kwargs):
    db = DBHelper()
    db.clean_database(DATABASE_NAME)

########################################### AIRFLOW DAG INFO ###############################################

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'email': ['glennljs@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=10),
    'provide_context': True
}

with DAG(
    'simulate_stock_streaming',
    default_args=default_args,
    description='mimick streaming of stock bids',
    schedule_interval=timedelta(minutes = 10),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['company_quant']
) as dag:
    combine_data_op = PythonOperator(
        task_id='combine_data',
        python_callable=combine_data,
    )

    generate_predictions_op = PythonOperator(
        task_id='generate_predictions',
        python_callable=generate_predictions,
    )

    save_model_op = PythonOperator(
        task_id='save_model',
        python_callable=save_model,
    )

    push_to_db_op = PythonOperator(
        task_id='push_to_db',
        python_callable=push_to_db,
    )

    clean_db_op = PythonOperator(
        task_id='clean_db',
        python_callable=clean_db,
    )

    load_to_gcs = BashOperator(
        task_id='load_to_gcs',
        bash_command="/home/airflow/3107-pipeline/upload/load_gcs.sh"
    )

    load_to_bq = BashOperator(
        task_id='load_to_bq',
        bash_command="/home/airflow/3107-pipeline/upload/load_bq.sh"
    )

    combine_data_op >> generate_predictions_op >> [save_model_op, push_to_db_op] \
    >> load_to_gcs >> load_to_bq >> clean_db_op 
