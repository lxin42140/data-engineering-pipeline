from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from sklearn.linear_model import PassiveAggressiveRegressor

from transformers import AutoTokenizer, AutoModelForSequenceClassification
from DBHelper import DBHelper
from LDA_retrain import lda_dataprep
from S3Helper import get_s3

import pandas as pd
import numpy as np
import pickle
import datetime as dt
import datetime
from datetime import datetime, timedelta
from textwrap import dedent
import json
import io
from io import StringIO
import requests
from gensim.models import CoherenceModel
import nltk
import torch
import snscrape.modules.twitter as sntwitter
import yfinance as yf

# nltk.download('wordnet')
# nltk.download('omw-1.4')


############################################# DB HELPER ###########################################

DATABASE_NAME = "industry-analysis"

UNFILCOL = 'sm_raw_twitter_unfiltered'
FILCOL = 'sm_raw_twitter_filtered'
SENTCOL = 'sm_sentiments_twitter'

SECTOR_ETF_PRICES = "sector-etf-prices"
SECTOR_PERC_CHANGE = "sector-perc-change"
SECTOR_RECOMMENDATIONS = "sector-recommendation"

#extracted collections
EXTRACT_SENTIMENTS = 'industry-sentiments'
EXTRACT_INDUSTRY_PRICE = 'industry-price'
EXTRACT_INDUSTRY_PERC_CHANGE = 'industry-perc-change'
EXTRACT_INDUSTRY_RECOMMENDATION = 'industry-recommendation'

############################################# CONSTANTS ###########################################

NUM_TOPICS = 15
TOPICS = ["0", "1", "2", "3", "6", "7", "8", "12", "13"]

############################################# SOCIAL MEDIA ###########################################


def twitter_scrape():
    # users
    searches = ['Energy', 'Basic Materials', 'Industrial Goods',
                'Consumer Cyclical', 'Consumer Defensive', 'Healthcare',
                'Financial', 'Technology', 'Communication Services',
                'Utilities', 'Real Estate']

    # Write tweets into list
    lst = []
    for search in searches:
        keyword = search + ' Sector'
        for i, tweet in enumerate(sntwitter.TwitterSearchScraper(f'{keyword} lang:en since:{dt.datetime.today().strftime("%Y-%m-%d")} -filter:links -filter:replies').get_items()):
            lst.append({'tweet_id': tweet.id, 'date': tweet.date,
                       'content': tweet.content, 'sector': search})

    test_twitter_scrape(lst)

    # update database only if there are tweets for the day
    if len(lst) > 0:
        db = DBHelper()
        # unfiltered data for mongo intermediary storage
        db.insert_many_for_collection(DATABASE_NAME, UNFILCOL, lst)
        # store in LDA training data on s3 as a csv file
        df2 = pd.DataFrame(lst)
        s3 = get_s3()
        dfobj = s3.Object('is3107-models', 'lda_train_data.csv')
        df = pd.read_csv(io.BytesIO(dfobj.get()['Body'].read()))
        out = df.append(df2).drop_duplicates(subset=['tweet_id'])
        csv_buffer = StringIO()
        out.to_csv(csv_buffer)
        dfobj.put(Body=csv_buffer.getvalue())

############################################ LDA ###########################################


def lda_filtering():

    s3 = get_s3()
    ldaobj = s3.Object('is3107-models', 'lda.pkl').get()
    lda = pickle.loads(ldaobj['Body'].read())
    dictobj = s3.Object('is3107-models', 'dict.pkl').get()
    dictionary = pickle.loads(dictobj['Body'].read())
    # get data
    db = DBHelper()
    collection = db.get_documents_for_collection(DATABASE_NAME, UNFILCOL, columns={'_id': 0, 'tweet_id': 1, 'content': 1, 'date': 1, 'sector':1})
    maindf = pd.DataFrame(list(collection))

    # no data ; terminate operation
    if len(maindf) == 0:
        return

    # LDA

    # data formatting
    df = lda_dataprep(maindf)

    # get corpus
    text_data = df.content.apply(lambda x: [w for w in x.split()])
    corpus = [dictionary.doc2bow(text) for text in text_data]

    # filter for topics
    train_vecs = []
    for i in range(len(df)):
        top_topics = (lda.get_document_topics(
            corpus[i], minimum_probability=0.0))
        topic_vec = [top_topics[i][1] for i in range(15)]
        train_vecs.append(topic_vec)

    for i in range(len(lda.show_topics())):
        label = str(i)
        df[label] = [train_vecs[x][i] for x in range(len(train_vecs))]

    print(df.head())

    df['topic'] = df.iloc[:, 4:].idxmax(axis=1)

    topics = TOPICS
    df = df[df['topic'].isin(topics)]
    maindf = maindf[maindf["tweet_id"].isin(df["tweet_id"].tolist())]

    # collection with filtered data
    db.insert_many_for_collection(
        DATABASE_NAME, FILCOL, maindf.to_dict('records'))

############################################# BERT ###########################################


def get_sentiments():
    db = DBHelper()
    collection = db.get_documents_for_collection(DATABASE_NAME, FILCOL, columns={'_id': 0, 'tweet_id': 1, 'content': 1, 'date': 1, 'sector':1})
    maindf = pd.DataFrame(list(collection))

    # no data ; terminate operation
    if len(maindf) == 0:
        return

    tokenizer = AutoTokenizer.from_pretrained("ProsusAI/finbert")
    model = AutoModelForSequenceClassification.from_pretrained(
        "ProsusAI/finbert")

    inputs = tokenizer(maindf['content'].tolist(
    ), padding=True, truncation=True, return_tensors='pt')
    outputs = model(**inputs)
    predictions = torch.nn.functional.softmax(outputs.logits, dim=-1)

    positive = predictions[:, 0].tolist()
    negative = predictions[:, 1].tolist()
    neutral = predictions[:, 2].tolist()

    maindf['positive'] = positive
    maindf['negative'] = negative
    maindf['neutral'] = neutral

    maindf = maindf.groupby(['date', "sector"]).agg(lambda x: list(x)).reset_index()

    db = DBHelper()
    db.insert_many_for_collection(
        DATABASE_NAME, SENTCOL, maindf.to_dict('records'))

########################################### SECTOR ETF PRICES ###############################################


# define sector + ETF ticker
etf_sectors = {'Energy': 'XLE',
               'Basic Materials': 'XLB',
               'Industrial Goods': 'XLI',
               'Consumer Cyclical': 'XLY',
               'Consumer Defensive': 'XLP',
               'Healthcare': 'XLV',
               'Financial': 'XLF',
               'Technology': 'SMH',
               'Communication Services': 'XTL',
               'Utilities': 'XLU',
               'Real Estate': 'VNQ'}

# convert sector + ETF ticker dictionary to dataframe
sector_df = pd.DataFrame.from_dict(etf_sectors, orient='index').reset_index()
sector_df.columns = ['sector', 'ETF']


def get_etf_prices(**kwargs):
    etf_prices = pd.DataFrame()
    all_etf_prices = []

    for ticker in etf_sectors.values():
        data = yf.download(ticker, period='2d').reset_index().tail(1)

        # error handling - None for tickers that cannot be found
        if len(data) > 0:
            data['ETF'] = ticker
            data['sector'] = list(etf_sectors.keys())[
                list(etf_sectors.values()).index(ticker)]
        else:
            data = pd.DataFrame([[None, None, None, None, None, None, None]], columns=['Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume'])
            data['ETF'] = ticker
            data['sector'] = list(etf_sectors.keys())[
                list(etf_sectors.values()).index(ticker)]
        etf_prices = etf_prices.append(data)

    all_etf_prices = etf_prices.to_json(orient='records')
    all_etf_prices = json.loads(all_etf_prices)
    json.dumps(all_etf_prices)

    if len(all_etf_prices) > 0:
        db = DBHelper()
        # push to temp storage
        db.insert_many_for_collection(
            DATABASE_NAME, SECTOR_ETF_PRICES, all_etf_prices)

########################################### SECTOR PERFORMANCE PERCENTAGE CHANGE DATA ###############################################


def get_sector_perc_change(**kwargs):
    fmp_API_KEY = 'bd99838c1aacaa89245372f4b4e4eae6'
    url = 'https://financialmodelingprep.com/api/v3/stock/sectors-performance?apikey=' + fmp_API_KEY
    sector_per_request = requests.get(url)
    sector_per_response = sector_per_request.json()

    # update sector names to be the same as previous functions
    sector_perc_change = pd.DataFrame(sector_per_response['sectorPerformance'])
    sector_perc_change.loc[len(sector_perc_change)] = ['Industrial Goods',
                                                       sector_perc_change.loc[sector_perc_change.sector == 'Industrials', 'changesPercentage'].values[0]]
    sector_perc_change = sector_perc_change.drop(
        sector_perc_change[sector_perc_change.sector == 'Industrials'].index)

    sector_perc_change.loc[len(sector_perc_change)] = ['Financial',
                        sector_perc_change.loc[sector_perc_change.sector == 'Financial Services', 'changesPercentage'].values[0]]
    sector_perc_change = sector_perc_change.drop(sector_perc_change[sector_perc_change.sector == 'Financial Services'].index)
    
    # None for sectors that are not cannot be found
    df_sectors = list(sector_perc_change['sector'])
    ori_sectors = list(etf_sectors.keys())
    missing_sectors = set(ori_sectors).difference(set(df_sectors))
    for i in missing_sectors:
        sector_perc_change = sector_perc_change.append({
            'sector': i,
            'changesPercentage': '0%',
        }, ignore_index=True)

    sector_perc_change = sector_perc_change.to_json(orient='records')
    sector_perc_change = json.loads(sector_perc_change)
    json.dumps(sector_perc_change)

    if len(sector_perc_change) > 0:
        db = DBHelper()
        # push to temp storage
        db.insert_many_for_collection(
            DATABASE_NAME, SECTOR_PERC_CHANGE, sector_perc_change)

########################################### RECOMEMNDATIONS USING QUANT DATA ###############################################


def generate_recommendations(**kwargs):
    db = DBHelper()
    etf_prices = db.get_documents_for_collection(DATABASE_NAME, SECTOR_ETF_PRICES, columns={'_id':0, 'created_at': 0})
    sector_perc = db.get_documents_for_collection(DATABASE_NAME, SECTOR_PERC_CHANGE, columns={'_id':0, 'created_at': 0})

    # convert all json extracted to dataframes
    etf_prices = pd.DataFrame(etf_prices)#.drop(columns=['_id','created_at'])
    sector_perc = pd.DataFrame(sector_perc)#.drop(columns=['_id','created_at'])

    # changesPercentage column stored in string XX%, convert to float
    sector_perc['changesPercentage'] = sector_perc['changesPercentage'].astype(
        str).str[:-1].astype(float)

    # merge all dataframes together
    train_df = etf_prices.merge(sector_perc, on='sector')

    etf_tickers = train_df.ETF.unique()
    adj_close_prices = list(train_df.groupby('sector').tail()['Adj Close'])
    sector_reco = []

    # filter out feature columns only
    train_features = train_df.drop(
        columns=['Adj Close', 'Date', 'ETF', 'sector']).columns

    s3 = get_s3()

    # load pre-trained scaler to scale feature columns
    scalerobj = s3.Object('is3107-models', 'etf_scaler.pkl').get()
    scaler = pickle.loads(scalerobj['Body'].read())
    # scaler = pickle.load(open('/home/airflow/3107-pipeline/static/etf_scaler.pkl', 'rb'))

    train_transform = scaler.transform(train_df[train_features])
    train_transform = pd.DataFrame(
        columns=train_features, data=train_transform, index=train_df.index)
    train_transform = np.array(train_transform).reshape(
        train_transform.shape[0], 1, train_transform.shape[1])

    # load pre-trained model
    modelobj = s3.Object('is3107-models', 'etf_price_model.pkl').get()
    model = pickle.loads(modelobj['Body'].read())
    new_pred = model.predict(train_transform)

    # include sector, and predicted price in output
    for i in range(len(new_pred)):
        pred_dict = {
            'ETF': etf_tickers[i],
            'sector': list(etf_sectors.keys())[list(etf_sectors.values()).index(etf_tickers[i])],
            'predicted_price': float(new_pred[i][0]),
            'adj_close': adj_close_prices[i]
        }
        sector_reco.append(pred_dict)
    print(sector_reco)

    # generate recommendation
    for sector in sector_reco:
        if sector['predicted_price'] >= sector['adj_close']:
            sector['sector_recommendation'] = 'BUY'
        elif sector['predicted_price'] < sector['adj_close']:
            sector['sector_recommendation'] = 'SELL'
        else:
            sector['sector_recommendation'] = 'HOLD'

    test_recommendations(sector_reco)

    if len(sector_reco) > 0:
        # push to temp storage
        db.insert_many_for_collection(
            DATABASE_NAME, SECTOR_RECOMMENDATIONS, sector_reco)

############################################# PREPARE TO LOAD TO BIGQUERY ###########################################


def prepare_collections(**kwargs):
    db = DBHelper()

    sectors = ['Energy', 'Basic Materials', 'Industrial Goods',
                'Consumer Cyclical', 'Consumer Defensive', 'Healthcare',
                'Financial', 'Technology', 'Communication Services',
                'Utilities', 'Real Estate']
     
    industry_sentiments = db.get_documents_for_collection(DATABASE_NAME, SENTCOL)
    industry_etf_prices = db.get_documents_for_collection(DATABASE_NAME, SECTOR_ETF_PRICES)
    industry_perc_change = db.get_documents_for_collection(DATABASE_NAME, SECTOR_PERC_CHANGE)
    industry_recommendations = db.get_documents_for_collection(DATABASE_NAME, SECTOR_RECOMMENDATIONS)

    extract_industry_sentiments = []
    extract_industry_etf_prices = []
    extract_industry_perc_change = []
    extract_industry_recommendations = []

    for sector in sectors:
        industry_etf_price = [x for x in industry_etf_prices if x['sector'] == sector][0]
        industry_perc = [x for x in industry_perc_change if x['sector'] == sector][0]
        industry_recommendation = [x for x in industry_recommendations if x['sector'] == sector][0]
        industry_sentiment = [x for x in industry_sentiments if x['sector'] == sector]
        # some days may not have tweets about a certain sector
        if len(industry_sentiment) > 0:
            industry_sentiment = industry_sentiment[0]
            extract_industry_sentiments.append({
                'sector': industry_sentiment['sector'],
                'tweet_id': industry_sentiment['tweet_id'],
                'tweet_content': industry_sentiment['content'],
                'positive_sentiment': industry_sentiment['positive'],
                'negative_sentiment': industry_sentiment['negative'],
                'neutral_sentiment': industry_sentiment['neutral'],
                'timestamp': industry_sentiment['date']
            })
       
        extract_industry_etf_prices.append({
            'sector': industry_etf_price['sector'],
            'open': industry_etf_price['Open'],
            'high': industry_etf_price['High'],
            'low': industry_etf_price['Low'],
            'close': industry_etf_price['Close'],
            'adj_close': industry_etf_price['Adj Close'],
            'volume': industry_etf_price['Volume'],
            'timestamp': datetime.today().strftime('%Y-%m-%d')
        })

        extract_industry_perc_change.append({
            'sector': industry_perc['sector'],
            'perc_change': industry_perc['changesPercentage'],
            'timestamp': datetime.today().strftime('%Y-%m-%d')
        })

        extract_industry_recommendations.append({
            'sector': industry_recommendation['sector'],
            'predicted_price': industry_recommendation['predicted_price'],
            'sector_recommendation': industry_recommendation['sector_recommendation'],
            'timestamp': datetime.today().strftime('%Y-%m-%d')
        })
    
    if len(extract_industry_sentiments) > 0:
        db.insert_many_for_collection(DATABASE_NAME, EXTRACT_SENTIMENTS, extract_industry_sentiments)
    if len(extract_industry_etf_prices) > 0:
        db.insert_many_for_collection(DATABASE_NAME, EXTRACT_INDUSTRY_PRICE, extract_industry_etf_prices)
    if len(extract_industry_perc_change) > 0:
        db.insert_many_for_collection(
            DATABASE_NAME, EXTRACT_INDUSTRY_PERC_CHANGE, extract_industry_perc_change)
    if len(extract_industry_recommendations) > 0:
        db.insert_many_for_collection(
            DATABASE_NAME, EXTRACT_INDUSTRY_RECOMMENDATION, extract_industry_recommendations)

############################################# LOAD TO BIGQUERY ###########################################


def extract_collections_to_json():
    db = DBHelper()
    collections_to_extract = [EXTRACT_SENTIMENTS, EXTRACT_INDUSTRY_PRICE, EXTRACT_INDUSTRY_PERC_CHANGE, EXTRACT_INDUSTRY_RECOMMENDATION]

    for collection in collections_to_extract:
        db.export_collection_to_json(DATABASE_NAME, collection)

########################################### CLEAN UP ###########################################

# clear all relevant temporary storage after data loaded to warehouse


def clean_up_database():
    db = DBHelper()
    db.clean_database(DATABASE_NAME)

########################################### UNIT TESTS ###########################################

def test_twitter_scrape(tweets):
    assert type(tweets) == list
    if len(tweets) > 0:
        for key in ['tweet_id', 'date', 'content', 'sector']:
            assert key in tweets[0] 

def test_recommendations(sector_reco):
    assert type(sector_reco) == list
    if len(sector_reco) > 0:
        for key in ['ETF', 'sector', 'predicted_price', 'adj_close', 'sector_recommendation']:
            assert key in sector_reco[0]

############################################# DAG ###########################################
default_args = {
    'owner': 'wenqi',
    'depends_on_past': False,
    'email': ['lwenqi.wql@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': dt.timedelta(minutes=5)
}
industry_analysis_dag = DAG(
    'industry_analysis',
    description='Scrape qualitative and quantitative industry data',
    schedule_interval=dt.timedelta(days=1),
    start_date=dt.datetime(2021, 1, 1),
    catchup=False,
    tags=['industry', 'qualitative', 'quantitative'],
)

############################################# EXTRACT ###########################################

twitter_scrape = PythonOperator(
    task_id='twitter_scrape',
    python_callable=twitter_scrape,
    dag=industry_analysis_dag
)

get_etf_prices = PythonOperator(
    task_id='get_etf_prices',
    python_callable=get_etf_prices,
    dag=industry_analysis_dag
)

get_sector_perc_change = PythonOperator(
    task_id='get_sector_perc_change',
    python_callable=get_sector_perc_change,
    dag=industry_analysis_dag
)

############################################# PROCESS ###########################################

lda_filtering = PythonOperator(
    task_id='lda_filtering',
    python_callable=lda_filtering,
    dag=industry_analysis_dag
)

get_sentiments = PythonOperator(
    task_id='get_sentiments',
    python_callable=get_sentiments,
    dag=industry_analysis_dag
)

generate_recommendations = PythonOperator(
    task_id='generate_recommendations',
    python_callable=generate_recommendations,
    dag=industry_analysis_dag
)

############################################# LOAD (UNCOMMENT IF USING) ###########################################

prepare_collections_for_loading = PythonOperator(
    task_id='prepare_collections_for_loading',
    python_callable=prepare_collections,
    dag=industry_analysis_dag
)

extract_collections = PythonOperator(
    task_id='extract_collections',
    python_callable=extract_collections_to_json,
    dag=industry_analysis_dag
)

load_to_gcs = BashOperator(
    task_id='load_to_gcs',
    bash_command="/home/airflow/3107-pipeline/upload/load_gcs.sh ",
    dag=industry_analysis_dag
)

load_to_bq = BashOperator(
    task_id='load_to_bq',
    bash_command="/home/airflow/3107-pipeline/upload/load_bq.sh ",
    dag=industry_analysis_dag
)

############################################# CLEAN UP ###########################################

clean_database = PythonOperator(
    task_id='clean_database',
    python_callable=clean_up_database,
    dag=industry_analysis_dag
)

################################## GRAPH ##################################
twitter_scrape >> lda_filtering >> get_sentiments >> prepare_collections_for_loading
[get_etf_prices, get_sector_perc_change] >> generate_recommendations >> prepare_collections_for_loading
prepare_collections_for_loading >> extract_collections >> load_to_gcs >> load_to_bq >> clean_database
