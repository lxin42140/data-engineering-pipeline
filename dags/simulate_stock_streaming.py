from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import random
import json
import codecs
from DBHelper import DBHelper
import random
import json
import codecs
import yfinance as yf
import time

from dags.company_fundamental_analysis import test_calculate_news_sentiments

########################################### DB HELPER ###############################################

DATABASE_NAME = "company-short-term-analysis"
COMPANY_STREAMING_QUOTES = 'company-streaming-quotes'

########################################### STATIC FUNCTIONS ###############################################

def random_select_company(companies):
    index = int(random.random() * (len(companies) - 1))
    selected_company = companies[index]['tradingCode'] + ".SI"

    print(f"Selected company: {selected_company}")
    return selected_company

def get_company_data(company):
    stock = yf.Ticker(company)

    company_data = stock.info

    if company_data is None:
        return None

    attr_to_keep = ['askSize', 'bidSize']
    output = { key: company_data[key] if key in company_data else -1 for key in attr_to_keep }
    output['company'] = company
    output['timestamp'] = datetime.now()

    test_get_company_data(output)
    return output

########################################### DAG FUNCTIONS ###############################################

def get_companies(**kwargs):
    companies_json = json.load(codecs.open('/home/airflow/3107-pipeline/static/companies.json', 'r', 'utf-8-sig'))
    companies = companies_json['companies']

    kwargs['ti'].xcom_push(key='companies', value=companies)

def randomize_num_runs(**kwargs):
    num_runs = random.randint(1, 10)
    test_num_rums(num_runs)
    print(f"Number of runs: {num_runs}")
    kwargs['ti'].xcom_push(key='num_runs', value=num_runs)

def randomize_interval_time(**kwargs):
    num_runs = kwargs['ti'].xcom_pull(key='num_runs', task_ids='randomize_num_runs')
    max_interval = 10 / num_runs
    intervals = []
    for _ in range(num_runs):
        interval = random.random() * max_interval
        intervals.append(interval)

    test_interval_time(intervals)
    print(f"Intervals: {intervals}")
    kwargs['ti'].xcom_push(key='intervals', value=intervals)

def get_data(**kwargs):
    companies = kwargs['ti'].xcom_pull(key='companies', task_ids='get_companies')
    intervals = kwargs['ti'].xcom_pull(key='intervals', task_ids='randomize_interval_time')
    batch_data = []

    for interval in intervals:
        selected_company = random_select_company(companies)
        data = get_company_data(selected_company)

        if (data is not None):
            print(f"{selected_company} data: {data}")
            batch_data.append(data)
        
        time.sleep(60 * interval)

    kwargs['ti'].xcom_push(key='batch_data', value=batch_data)
    
def group_data(**kwargs):
    batch_data = kwargs['ti'].xcom_pull(key='batch_data', task_ids='get_data')
    grouped_data = {}

    for company_data in batch_data:
        if company_data['company'] not in grouped_data:
            company_symbol = company_data['company']
            grouped_data[company_symbol] = company_data
            grouped_data['num_values'] = 1
        else:
            for key, value in company_data.items():
                if key == 'company' or key == 'timestamp':
                    continue
                grouped_data[company_data['company']][key] += value
            grouped_data['num_values'] += 1

    print(f"Grouped data: {grouped_data}")
    kwargs['ti'].xcom_push(key='grouped_data', value=grouped_data)

def aggregate_data(**kwargs):
    ## this function just averages data if there is more than one instance of the same company
    grouped_data = kwargs['ti'].xcom_pull(key='grouped_data', task_ids='group_data')

    for company in grouped_data:
        if company['num_values'] > 1:
            for key, value in company.items():
                if key in ['num_values', 'company', 'timestamp']:
                    continue
                company[key] = value / company['num_values'] 

    aggregated_data = grouped_data.values()

    kwargs['ti'].xcom_push(key='aggregated_data', value=aggregated_data)

def push_to_db(**kwargs):
    data = kwargs['ti'].xcom_pull(key='aggregated_data', task_ids='aggregate_data')

    db = DBHelper()
    db.insert_many_for_collection(DATABASE_NAME, COMPANY_STREAMING_QUOTES, data)

########################################### UNIT TESTS ###############################################

def test_num_rums(num_runs):
    assert num_rums => 1 and num_rums <= 10

def test_interval_time(interval):
    assert type(interval) == list
    assert len(interval) == 10
    assert sum(interval) <= 60 * 10

def test_get_company_data(data):
    assert type(data) == dict
    assert 'askSize' in data
    assert 'bidSize' in data
    assert 'company' in data
    assert 'timestamp' in data

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

    get_companies_operator = PythonOperator(
        task_id='get_companies',
        python_callable=get_companies
    )

    randomize_num_runs_operator = PythonOperator(
        task_id='randomize_num_runs',
        python_callable=randomize_num_runs
    )

    randomize_interval_time_operator = PythonOperator(
        task_id='randomize_interval_time',
        python_callable=randomize_interval_time
    )

    get_data_operator = PythonOperator(
        task_id='get_data',
        python_callable=get_data,
    )

    group_data_operator = PythonOperator(
        task_id='group_data',
        python_callable=group_data,
    )

    aggregate_data_operator = PythonOperator(
        task_id='aggregate_data',
        python_callable=aggregate_data,
    )

    push_to_db_operator = PythonOperator(
        task_id='push_to_db',
        python_callable=push_to_db,
    )

    get_companies_operator >> randomize_num_runs_operator >> randomize_interval_time_operator \
    >> get_data_operator >> aggregate_data_operator >> push_to_db_operator
