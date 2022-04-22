from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from pathlib import Path

import requests
import random
import json
import codecs
from DBHelper import DBHelper
import snscrape.modules.twitter as sntwitter
import snscrape.modules.telegram as snTele
from nltk.sentiment import SentimentIntensityAnalyzer
import pytz
import random
import json
from pytrends.request import TrendReq
import codecs
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
import social_media_spam_classifier as SocialMediaSpamClassifier
import pandas as pd
import numpy as np
import yahoo_fin.stock_info as si
import talib as ta
import yfinance as yf
from adapted_stocker import predict_stock_price_for_tomorrow
import glob

NLTK_DIR = "/home/airflow/nltk_data"
if not glob.glob(f"{NLTK_DIR}/sentiment/vader_lexicon.*"):
    nltk.download('vader_lexicon')
########################################### DB HELPER ###############################################

DATABASE_NAME = "company-fundamental-analysis"

SM_RAW_TWITTER="sm-raw-twitter"
SM_RAW_TELEGRAM="sm-raw-telegram"
SM_FILTERED_TWITTER="sm-filtered-twitter"
SM_FILTERED_TELEGRAM="sm-filtered-telegram"
SM_COMBINED_PROCESSED="sm-combined-processed"
SOCIAL_MEDIA_SENTIMENTS='sm-sentiments'

NEWS_API='news-newsapi'
MEDIASTACK='news-mediastack'
NEWS_COMBINED='news-combined'
NEWS_SENTIMENTS='news-sentiments'

COMPANY_WIKI_TRENDS = 'company-wiki-trends'
COMBINED_SENTIMENTS = 'combined-sentiments'
COMPANY_PRICES = "company-prices"
COMPANY_RATIOS = "company-ratios"
COMPANY_TECH_INDICATORS = 'company-tech-indicators'
COMPANY_GOOGLE_TRENDS = 'company-google-trends'
COMPANY_RECOMMENDATION = 'company-recommendation'

#extracted collections
EXTRACT_COMPANY_ANALYSIS = 'company-analysis'
EXTRACT_SOCIAL_MEDIA_NEWS_SENTIMENT = 'social-media-news-sentiment'
EXTRACT_COMPANY_INTEREST = 'company-interest'
EXTRACT_COMPANY_PRICE = 'company-price'
EXTRACT_COMPANY_RATIO = 'company-ratio'
EXTRACT_COMPANY_TECHNICAL_INDICATORS = 'company-technical-indicators'

########################################### TARGET COMPANY ###############################################

def random_select_companies(**kwargs):
    companies_json = json.load(codecs.open('/home/airflow/3107-pipeline/static/companies.json', 'r', 'utf-8-sig'))
    companies_arr = companies_json['companies']

    selected_companies = []
    selected_tickers = []
    company_ticker_maps = []
    
    n = 10
    for i in range(n):
        index = int(random.random() * (len(companies_arr) - 1))
        if (companies_arr[index]['tradingName'] in selected_companies):
            n += 1
        else:
            # standardized all company names to lowercase
            selected_companies.append(companies_arr[index]['tradingName'].lower())
            # addded 'SI' to trading codes to access price data on yfinance
            selected_tickers.append(companies_arr[index]['tradingCode'] + ".SI")
            # map code to company name
            company_ticker_dict = {
                "company": companies_arr[index]['tradingName'].lower(),
                "ticker": (companies_arr[index]['tradingCode'] + ".SI")
            }
            company_ticker_maps.append(company_ticker_dict)

    print(selected_companies)
    print(selected_tickers)
    print(company_ticker_maps)
    
    kwargs['ti'].xcom_push(key='company_names', value=selected_companies)
    kwargs['ti'].xcom_push(key='company_tickers', value=selected_tickers)
    kwargs['ti'].xcom_push(key='company_ticker_maps', value=company_ticker_maps)

#utility function to get company name given ticker
def get_company_name_from_ticker(company_ticker_maps, ticker):
    for dict in company_ticker_maps:
        if dict['ticker'] == ticker:
            return dict['company']
    
    raise Exception(f"Unable To Find Company Name For {ticker}")
########################################### NEWS ###############################################

def get_newsapi_api(**kwargs):
    selected_companies=kwargs['ti'].xcom_pull(key='company_names', task_ids='random_select_companies')
    company_news = []
    newsapi_apikey = 'd315c85053c24c79a7f09bf6a694b125'
    yesterday = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    # selected_companies = [selected_companies[0]] #just to reduce api calls for testing, delete when done
    for company in selected_companies:
        response = requests.get(f'https://newsapi.org/v2/everything?q={company}&language=en&apiKey={newsapi_apikey}&from={yesterday}&to={yesterday}')
        
        if response.status_code == 200:
            data = response.json()
            articles = []
            for article in data['articles']:
                articles.append({
                    "author": article["author"],
                    "title": article["title"],
                    "description": article["description"],
                    "source": article["source"]
                })
            company_news.append({
                'company': company,
                'articles': articles
            })
        else:
            company_news.append({
                'company': company,
                'articles': []
            })
    
        test_get_news(data)
    
    if len(company_news) > 0:
        db = DBHelper()
        db.insert_many_for_collection(DATABASE_NAME, NEWS_API, company_news)


def get_mediastack_api(**kwargs):
    selected_companies=kwargs['ti'].xcom_pull(key='company_names', task_ids='random_select_companies')
    company_news = []
    mediastack_apikey ='a2e70c5f7d0de01c5f73af203d2d70e5'
    yesterday = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    # selected_companies = [selected_companies[0]] #just to reduce api calls for testing, delete when done
    for company in selected_companies:
        response = requests.get(f'http://api.mediastack.com/v1/news?access_key={mediastack_apikey}&languages=en&keywords={company}&date={yesterday}')

        if response.status_code == 200:
            data = response.json()
            articles = []
            for article in data['data']:
                articles.append({
                    "author": article["author"],
                    "title": article["title"],
                    "description": article["description"],
                    "source": article["source"]
                })
            company_news.append({
                'company': company,
                'data': articles
            })
        else:
            company_news.append({
                'company': company,
                'data': []
            })

        test_get_media()

    if len(company_news) > 0:
        db = DBHelper()
        db.insert_many_for_collection(DATABASE_NAME, MEDIASTACK, company_news)


def standardise_news_data(**kwargs):
    db = DBHelper()

    selected_companies=kwargs['ti'].xcom_pull(key='company_names', task_ids='random_select_companies')

    newsapi_documents = db.get_documents_for_collection(DATABASE_NAME, NEWS_API)
    mediastack_documents = db.get_documents_for_collection(DATABASE_NAME, MEDIASTACK)

    all_company_news = []

    for company in selected_companies:
        consolidated_articles = []
        for document in newsapi_documents:
            if document['company'] == company:
                for article in document['articles']:
                    standardised_details = {
                        "author": article["author"],
                        "title": article["title"],
                        "description": article["description"],
                        "source": article["source"]["name"]
                    }
                    consolidated_articles.append(standardised_details)

        for document in mediastack_documents:
            if document['company'] == company:
                for article in document['data']:
                    standardised_details = {
                        "author": article["author"],
                        "title": article["title"],
                        "description": article["description"],
                        "source": article["source"]
                    }
                    consolidated_articles.append(standardised_details)

        all_company_news.append({
            "company": company,
            "news": consolidated_articles
        })

    test_standardise_news(all_company_news)

    if len(all_company_news) > 0:
        db.insert_many_for_collection(DATABASE_NAME, NEWS_COMBINED, all_company_news)


def news_sentiment_analysis():    
    sia = SentimentIntensityAnalyzer()
    db = DBHelper()

    news_combined = db.get_documents_for_collection(DATABASE_NAME, NEWS_COMBINED)

    company_sentiments = []

    for company in news_combined:
        del company["_id"]
        neg = 0
        neu = 0
        pos = 0
        compound = 0
        for news in company["news"]:
            if news["description"] is not None:
                score = sia.polarity_scores(news["description"])
                neg = neg + score["neg"]
                neu = neu + score["neu"]
                pos = pos + score["pos"]
                compound = compound + score["compound"]
        
        number_of_news = len(company["news"])
        if (number_of_news > 0):
            company["neg_news_sentiments"] = neg / number_of_news
            company["pos_news_sentiments"] = pos / number_of_news
            company["neu_news_sentiments"] = neu / number_of_news
            company["compound_news_sentiments"] = compound / number_of_news
            company['number_relevant_news'] = number_of_news
        else:
            company["neg_news_sentiments"] = None
            company["pos_news_sentiments"] = None
            company["neu_news_sentiments"] = None
            company["compound_news_sentiments"] = None
            company['number_relevant_news'] = number_of_news
        company_sentiments.append(company)

    test_calculate_news_sentiments(company_sentiments)

    if len(company_sentiments) > 0:
        db.insert_many_for_collection(DATABASE_NAME, NEWS_SENTIMENTS, company_sentiments)

def social_media_sentiments(**kwargs):
    sia = SentimentIntensityAnalyzer()
    db = DBHelper()

    selected_companies=kwargs['ti'].xcom_pull(key='company_names', task_ids='random_select_companies')
    social_media = db.get_documents_for_collection(DATABASE_NAME, SM_COMBINED_PROCESSED)

    social_media_sentiment = []

    for company in selected_companies:
        twitter_pos=0
        twitter_neu=0
        twitter_neg=0
        twitter_compound=0
        telegram_pos=0
        telegram_neu=0
        telegram_neg=0
        telegram_compound=0
        number_relevant_tweets=0
        number_telegram_messages=0
        for document in social_media:
            if document['company'] == company:
                del document['_id']
                score = sia.polarity_scores(document["content"])
                if document['source'] == 'telegram':
                    number_telegram_messages += 1
                    telegram_pos += score['pos']
                    telegram_neu += score['neu']
                    telegram_neg += score['neg']
                    telegram_compound += score['compound']
                else:
                    number_relevant_tweets += 1
                    twitter_pos += score['pos']
                    twitter_neu += score['neu']
                    twitter_neg += score['neg']
                    twitter_compound += score['compound']

        total_count = number_relevant_tweets + number_telegram_messages
        
        if total_count > 0:
            social_media_sentiment.append({
                "company": company,
                "telegram_compound_sm_sentiments": telegram_compound / number_telegram_messages,
                "twitter_compound_sm_sentiments": twitter_compound / number_relevant_tweets,
                "pos_sm_sentiments": (twitter_pos + telegram_pos) / total_count,
                "neu_sm_sentiments": (twitter_neu + telegram_neu) / total_count,
                "neg_sm_sentiments": (twitter_neg + telegram_neg) / total_count,
                "compound_sm_sentiments": (twitter_compound + telegram_compound) / total_count,
                "number_relevant_tweets": number_relevant_tweets,
                "number_telegram_messages": number_telegram_messages
            })
        else:
            social_media_sentiment.append({
                "company": company,
                "telegram_compound_sm_sentiments": None,
                "twitter_compound_sm_sentiments": None,
                "pos_sm_sentiments": None,
                "neu_sm_sentiments": None,
                "neg_sm_sentiments": None,
                "compound_sm_sentiments": None,
                "number_relevant_tweets": number_relevant_tweets,
                "number_telegram_messages": number_telegram_messages
            })

    test_social_media_sentiments(social_media_sentiment)

    if len(social_media_sentiment) > 0:
        db.insert_many_for_collection(DATABASE_NAME, SOCIAL_MEDIA_SENTIMENTS, social_media_sentiment)

############################################# PYTRENDS ###########################################

def get_google_trends(**kwargs):
    selected_companies=kwargs['ti'].xcom_pull(key='company_names', task_ids='random_select_companies')

    pytrends = TrendReq(hl='en-US', tz=480)

    trends = []
    for i in range(len(selected_companies)):
        pytrends.build_payload([selected_companies[i]], cat=16, timeframe='now 1-d', geo='SG')
        interests = pytrends.interest_over_time() #returns a dataframe

        if (len(interests) > 0):
            interests_list =  interests.iloc[:,0].to_numpy().tolist()
            average = sum(interests_list) / len(interests_list)
        else:
            interests_list = []
            average = None

        trends.append({
            "company": selected_companies[i],
            "daily_interests": interests_list,
            "average": average
        })
    
    test_google_trends()

    if (len(trends) > 0):
        db = DBHelper()
        db.set_validation(False)
        db.insert_many_for_collection(DATABASE_NAME, COMPANY_GOOGLE_TRENDS, trends)
        
def get_company_wiki_trends(**kwargs):
    selected_companies=kwargs['ti'].xcom_pull(key='company_names', task_ids='random_select_companies')
    start = datetime.today().strftime('%Y%m%d')  
    end = datetime.today().strftime('%Y%m%d')  

    wiki_data = []
    companies_no_data = []
    for company in selected_companies:
        link = 'https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/en.wikipedia/all-access/all-agents' \
                '/{company}/daily/{st}/{end}'.format(company=company, st=start, end=end)
        r = requests.Session()
        r.headers = {"User-Agent": "lxin42140@gmail.com)"}
        try:
            response = r.get(link)
            response = response.json()
            for data in response['items']:
                wiki_dict = {}
                wiki_dict["number_views"] = data['views']
                wiki_dict["company"] = company
                wiki_data.append(wiki_dict)
        except Exception as e: # exception is thrown when there is no data
            companies_no_data.append(company)

    for company in companies_no_data:
        wiki_dict = {}
        wiki_dict["number_views"] = None
        wiki_dict["company"] = company
        wiki_data.append(wiki_dict)
        
    test_company_wiki_trends(wiki_data)

    if (len(wiki_data) > 0):
        db = DBHelper()
        db.insert_many_for_collection(DATABASE_NAME, COMPANY_WIKI_TRENDS , wiki_data)
        
############################################# SOCIAL MEDIA ###########################################

def get_tweets(**kwargs):
    companies=kwargs['ti'].xcom_pull(key='company_names', task_ids='random_select_companies')
    # companies = ['alibaba', 'paypal', 'tencent']

    search_string = companies[0]
    for i, name in enumerate(companies):
        if i == 0:
            pass
        else:
            search_string = search_string + " OR " + name

    tweets = []
    for i,tweet in enumerate(sntwitter.TwitterSearchScraper(f"{search_string} since:{datetime.today().strftime('%Y-%m-%d')} lang:en").get_items()):
        # for testing, only store 50
        if i>50:
            break
        
        for company in companies:
            if company in tweet['content'].lower():
                tweet_dict = {}
                tweet_dict['date'] = tweet.date
                tweet_dict['content'] = tweet.content.lower()
                tweet_dict['company'] = company
                tweet_dict['source'] = "twitter"
                tweets.append(tweet_dict)
    
    test_get_tweets(tweets)

    if len(tweets) > 0:
        dbHelper = DBHelper()
        dbHelper.insert_many_for_collection(DATABASE_NAME, SM_RAW_TWITTER, tweets)

def get_telegram_messages(**kwargs):
    companies=kwargs['ti'].xcom_pull(key='company_names', task_ids='random_select_companies')
    # companies = ['alibaba', 'paypal', 'tencent']
    telegram_groups = ['Bloomberg', 'SGX Invest', 'SG Market Updates', 'Business & Finance News USA Edition', 'The Real Rayner Teo', 'Seedly Personal Finance SG']

    def scrape_telegram_for_channel(channel):
        messages = []
        for i, message in enumerate(snTele.TelegramChannelScraper(channel).get_items()):
            if i > 50:
                break
            if message.date < datetime.now(tz=pytz.UTC):
                break
            if not message.content:
                continue

            # filter messages that do not mention the company name
            for company in companies:
                if company in message.content.lower():
                    message_dict = {}
                    message_dict['date'] = message.date
                    message_dict['content'] = message.content.lower()
                    message_dict['company'] = company
                    message_dict['source'] = "telegram"
                    messages.append(message_dict)
                    break

        return messages

    combined_messages = []
    for group in telegram_groups:
        combined_messages = combined_messages + scrape_telegram_for_channel(group)

    test_get_telegram_messages(combined_messages)

    if len(combined_messages) > 0:
        dbHelper = DBHelper()
        dbHelper.insert_many_for_collection(DATABASE_NAME, SM_RAW_TELEGRAM, combined_messages)


def filter_all_spams():
    dbHelper = DBHelper()

    twitter_documents = dbHelper.get_documents_for_collection(DATABASE_NAME, SM_RAW_TWITTER)
    telegram_documents = dbHelper.get_documents_for_collection(DATABASE_NAME, SM_RAW_TELEGRAM)

    combined_documents = twitter_documents + telegram_documents
    
    for document in combined_documents:
        del document["_id"]
    
    # filter out spams
    filter_sm_documents = [post for post in combined_documents if SocialMediaSpamClassifier.classify_spam(post['content']) == 1]

    if len(filter_sm_documents) > 0:
        dbHelper.insert_many_for_collection(DATABASE_NAME, SM_COMBINED_PROCESSED, filter_sm_documents)


################################### COMBINE SENTIMENTS #################################

def combine_sentiments(**kwargs):
    db = DBHelper()

    selected_companies=kwargs['ti'].xcom_pull(key='company_names', task_ids='random_select_companies')

    sm_sentiments = db.get_documents_for_collection(DATABASE_NAME, SOCIAL_MEDIA_SENTIMENTS)
    news_sentiments = db.get_documents_for_collection(DATABASE_NAME, NEWS_SENTIMENTS)
    google_company_trends = db.get_documents_for_collection(DATABASE_NAME, COMPANY_GOOGLE_TRENDS)
    wiki_company_trends = db.get_documents_for_collection(DATABASE_NAME, COMPANY_WIKI_TRENDS)

    number_views = [x['number_views'] for x in wiki_company_trends if x['number_views'] is not None]
    max_view = max(number_views) if len(number_views) > 0 else None

    for company in wiki_company_trends:
        number_views = company['number_views']
        if number_views is not None and max_view is not None:
            company['norm_number_views'] = (float(number_views)/max_view) * 100 if max_view > 0 else 0
        else:
            company['norm_number_views'] = None

    weighted_score = []

    for company in selected_companies:
        google_trends = [x for x in google_company_trends if x['company'] == company]
        wiki_trends = [x for x in wiki_company_trends if x['company'] == company]
        trends = []
        if  len(google_trends) > 0 and google_trends[0]['average'] is not None:
            trends.append(google_trends[0]['average'])
        if len(wiki_trends) > 0 and wiki_trends[0]['norm_number_views'] is not None:
            trends.append(wiki_trends[0]['norm_number_views'])

        combined_trend = sum(trends) / float(len(trends)) if len(trends) > 0 else None

        news_company_res = [x for x in news_sentiments if x['company'] == company]
        sm_company_res = [x for x in sm_sentiments if x['company'] == company]

        if len(news_company_res) > 0:
            news_company = news_company_res[0]
            if len(sm_company_res) > 0:
                sm_company = sm_company_res[0]
                sentiments = []
                if sm_company['compound_sm_sentiments'] is not None: sentiments.append(sm_company['compound_sm_sentiments'])
                if news_company['compound_news_sentiments'] is not None: sentiments.append(news_company['compound_news_sentiments'])
                # pos_combined_sentiments = (sm_company['pos_sm_sentiments'] + news_company['pos_news_sentiments']) / 2
                # neu_combined_sentiments = (sm_company['neu_sm_sentiments'] + news_company['neu_news_sentiments']) / 2
                # neg_combined_sentiments = (sm_company['neg_sm_sentiments'] + news_company['neg_news_sentiments']) / 2
                compound_combined_sentiments = sum(sentiments) / float(len(sentiments)) if len(sentiments) > 0 else None
                number_relevant_tweets = sm_company['number_relevant_tweets']
                number_telegram_messages = sm_company['number_telegram_messages']
            else:
                # pos_combined_sentiments = news_company['pos_news_sentiments']
                # neu_combined_sentiments = news_company['neu_news_sentiments']
                # neg_combined_sentiments = news_company['neg_news_sentiments']
                compound_combined_sentiments = news_company['compound_news_sentiments'] if news_company['compound_news_sentiments'] is not None else None
                number_relevant_tweets = 0
                number_telegram_messages = 0


        weighted_score.append({
            'company': company,
            'compound_combined_sentiments': compound_combined_sentiments,
            'combined_trend': combined_trend,
            'google_search_frequency_interest': google_trends[0]['average'] if len(google_trends) > 0 and google_trends[0]['average'] else None,
            'wiki_search_frequency': wiki_trends[0]['number_views'] if len(wiki_trends) > 0 and  wiki_trends[0]['number_views'] is not None else None,
            'public_sentiment_score': combined_trend * compound_combined_sentiments if combined_trend is not None and compound_combined_sentiments is not None else None,
            'number_relevant_tweets': number_relevant_tweets,
            'number_telegram_messages': number_telegram_messages
        })

    test_combine_sentiments(weighted_score)

    if len(weighted_score) > 0:
        db.insert_many_for_collection(DATABASE_NAME, COMBINED_SENTIMENTS, weighted_score)
        
########################################### COMPANY STOCK PRICES ###############################################

def get_company_prices(**kwargs):
    company_prices = pd.DataFrame()
    all_company_prices = []

    selected_tickers=kwargs['ti'].xcom_pull(key='company_tickers', task_ids='random_select_companies')
    company_ticker_maps = kwargs['ti'].xcom_pull(key='company_ticker_maps', task_ids='random_select_companies')

    for ticker in selected_tickers:
        data = yf.download(ticker, period='2d').reset_index().tail(1)

        if len(data) > 0:
            data['ticker'] = ticker  # add this column because the dataframe doesn't contain a column with the ticker
            data['company'] = get_company_name_from_ticker(company_ticker_maps, ticker)
        else:
            data = pd.DataFrame([[None, None, None, None, None, None, None]], columns=['Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume'])
            data['ticker'] = ticker
            data['company'] = get_company_name_from_ticker(company_ticker_maps, ticker)
        company_prices = company_prices.append(data)

    all_company_prices = company_prices.to_json(orient='records')
    all_company_prices = json.loads(all_company_prices)
    json.dumps(all_company_prices)

    if len(all_company_prices) > 0:
        db = DBHelper()
        db.insert_many_for_collection(DATABASE_NAME, COMPANY_PRICES, all_company_prices)

########################################### COMPANY FINANCIAL RATIOS ###############################################

def get_company_ratios(**kwargs):
    selected_tickers=kwargs['ti'].xcom_pull(key='company_tickers', task_ids='random_select_companies')
    company_ticker_maps = kwargs['ti'].xcom_pull(key='company_ticker_maps', task_ids='random_select_companies')

    # extracted relevant metrics for get_stats output
    metrics = ['Beta (5Y Monthly)', '52 Week High 3', 'Payout Ratio 4', 'Profit Margin', 'Return on Assets (ttm)', 
            'Return on Equity (ttm)', 'Gross Profit (ttm)', 'Gross Profit (ttm)', 'EBITDA',
            'Diluted EPS (ttm)', 'Quarterly Earnings Growth (yoy)', 'Total Debt/Equity (mrq)',
            'Current Ratio (mrq)', 'Book Value Per Share (mrq)', 'Operating Cash Flow (ttm)']
    
    all_metrics = ['Market Cap (intraday)', 'Enterprise Value', 'Trailing P/E', 'Forward P/E', 'PEG Ratio (5 yr expected)', 'Price/Sales (ttm)', 'Price/Book (mrq)', 'Enterprise Value/Revenue',
            'Enterprise Value/EBITDA', 'Beta (5Y Monthly)', '52 Week High 3', 'Payout Ratio 4', 'Profit Margin', 'Return on Assets (ttm)', 'Return on Equity (ttm)', 'Gross Profit (ttm)', 'Diluted EPS (ttm)', 
            'Quarterly Earnings Growth (yoy)', 'Total Debt/Equity (mrq)', 'Current Ratio (mrq)', 'Book Value Per Share (mrq)', 'Operating Cash Flow (ttm)']
    
    all_ratios = []
    present_tickers = []

    for ticker in selected_tickers:
        try:
            stats_val = si.get_stats_valuation(ticker)
            temp_dict_val = dict(zip(stats_val[0], stats_val[1]))
            
            stats = si.get_stats(ticker)
            temp_dict_stats = dict(zip(stats['Attribute'], stats['Value']))
            temp_dict_stats = dict((key, value) for key, value in temp_dict_stats.items() if key in metrics)
            
            #combine both dictionaries in 1 dictionary
            temp_dict_val.update(temp_dict_stats)
            
            temp_dict_val['ticker'] = ticker
            temp_dict_val['company'] = get_company_name_from_ticker(company_ticker_maps, ticker)

            present_tickers.append(ticker)
            all_ratios.append(temp_dict_val)
            
        except Exception as ex:
            print('Cannot find this ticker!')
            
    # error handling - null for tickers that cannot be found
    for ticker in set(selected_tickers).difference(set(present_tickers)):
        temp_dict_val = {
                'company': get_company_name_from_ticker(company_ticker_maps, ticker),
                'ticker': ticker, 
        }
        for metric in all_metrics:
            temp_dict_val[metric] = None
            
        all_ratios.append(temp_dict_val)
    
    # replace all np.nan values with None for mongodb
    all_ratios = pd.DataFrame(all_ratios).replace([np.nan], [None])
    all_ratios = all_ratios.to_json(orient='records')
    all_ratios = json.loads(all_ratios)
    json.dumps(all_ratios)

    if len(all_ratios) > 0:
        db = DBHelper()
        db.insert_many_for_collection(DATABASE_NAME, COMPANY_RATIOS, all_ratios)

########################################### COMPANY STOCK PRICES & TECHNICAL INDICATORS ###############################################

def get_company_tech_indicators(**kwargs): 
    selected_tickers=kwargs['ti'].xcom_pull(key='company_tickers', task_ids='random_select_companies')
    company_ticker_maps = kwargs['ti'].xcom_pull(key='company_ticker_maps', task_ids='random_select_companies')

    columns = ['Date', 'Day_50_SMA', 'Day_50_EMA', 'Day_50_WMA',
            'upper_band', 'middle_band', 'lower_band', 'SAR', 'MACD', 'MACD_signal',
            'MACD_hist', 'RSI', 'AROON_down', 'AROON_up', 'PPO', 'ADX', 'CCI',
            'OBV', 'ATR']

    present_tickers = []
    hist_company_prices = pd.DataFrame()

    for ticker in selected_tickers:
        data = yf.download(ticker, period='1y').reset_index()
        if len(data) > 0:
            data['ticker'] = ticker
            hist_company_prices = hist_company_prices.append(data)
            present_tickers.append(ticker)

    # use 1-year historical prices to calculate technical indicators
    all_tech_indicators = pd.DataFrame()

    for ticker in present_tickers:
        ticker_price_df = hist_company_prices[hist_company_prices['ticker'] == ticker]
        ticker_price_df['company'] = get_company_name_from_ticker(company_ticker_maps, ticker)

        # overlap studies indicators
        ticker_price_df['Day_50_SMA'] = ta.SMA(ticker_price_df['Close'], timeperiod=50)
        ticker_price_df['Day_50_EMA'] = ta.EMA(ticker_price_df['Close'], timeperiod=50)
        ticker_price_df['Day_50_WMA'] = ta.WMA(ticker_price_df['Close'], timeperiod=50)
        ticker_price_df['upper_band'], ticker_price_df['middle_band'], ticker_price_df['lower_band'] = ta.BBANDS(ticker_price_df['Close'], timeperiod =50)
        ticker_price_df['SAR'] = ta.SAR(ticker_price_df['High'], ticker_price_df['Low'])

        # momentum indicators
        ticker_price_df['MACD'], ticker_price_df['MACD_signal'], ticker_price_df['MACD_hist']= ta.MACD(ticker_price_df['Close'], 
                                                                                                fastperiod=12, slowperiod=26, signalperiod=9)
        ticker_price_df['RSI'] = ta.RSI(ticker_price_df['Close'])
        ticker_price_df['AROON_down'], ticker_price_df['AROON_up'] = ta.AROON(ticker_price_df['High'], ticker_price_df['Low'])
        ticker_price_df['PPO'] = ta.PPO(ticker_price_df['Close'])
        ticker_price_df['ADX'] = ta.ADX(ticker_price_df['High'], ticker_price_df['Low'], ticker_price_df['Close'])
        ticker_price_df['CCI'] = ta.CCI(ticker_price_df['High'], ticker_price_df['Low'], ticker_price_df['Close'])

        # volume indicators 
        ticker_price_df['OBV'] = ta.OBV(ticker_price_df['Close'], ticker_price_df['Volume'])

        # volatility indicators
        ticker_price_df['ATR'] = ta.ATR(ticker_price_df['High'], ticker_price_df['Low'], ticker_price_df['Close'])

        all_tech_indicators = all_tech_indicators.append(ticker_price_df, ignore_index=True)

        # drop stock price and volume columns
        all_tech_indicators = all_tech_indicators.drop(columns=['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume'])
    
    # error handling - nan for tickers that cannot be found
    for ticker in set(selected_tickers).difference(set(present_tickers)):
        temp_dict_tech_ind = {
                'company': get_company_name_from_ticker(company_ticker_maps, ticker),
                'ticker': ticker, 
        }
        for column in columns:
            temp_dict_tech_ind[column] = None
        
        all_tech_indicators = all_tech_indicators.append(temp_dict_tech_ind, ignore_index=True)

    # extract the row with maximum dates for all stocks (i.e. today's technical indicators)        
    tech_indicators_today = all_tech_indicators.groupby('ticker').tail(1)

    tech_indicators_today = tech_indicators_today.to_json(orient='records')
    tech_indicators_today = json.loads(tech_indicators_today)
    json.dumps(tech_indicators_today)

    if len(tech_indicators_today) > 0:
        db = DBHelper()
        db.insert_many_for_collection(DATABASE_NAME, COMPANY_TECH_INDICATORS, tech_indicators_today)

########################################### GENERATE RECOMMENDATION ###############################################

def generate_recommendation(**kwargs):
    selected_tickers = kwargs['ti'].xcom_pull(key='company_tickers', task_ids='random_select_companies')
    company_ticker_maps = kwargs['ti'].xcom_pull(key='company_ticker_maps', task_ids='random_select_companies')
    
    db = DBHelper()
    company_sentiments = db.get_documents_for_collection(DATABASE_NAME, COMBINED_SENTIMENTS)
    company_prices = db.get_documents_for_collection(DATABASE_NAME, COMPANY_PRICES)
    
    recommendations = []
    for ticker in selected_tickers:
        recommend_dict = {}
        recommend_dict['ticker'] = ticker
        recommend_dict['company'] = get_company_name_from_ticker(company_ticker_maps, ticker)

        # get predicted stock price for tomorrow
        try:
            recommend_dict['predicted_tomorrow_price'] = predict_stock_price_for_tomorrow(ticker)
        except Exception as e:
            recommend_dict['predicted_tomorrow_price'] = None
        
        # find sentiment score
        sentiment_score = None
        for sentiment in company_sentiments:
            if sentiment['company'] == recommend_dict['company']:
                sentiment_score = sentiment['public_sentiment_score']
                break
        
        # find company price
        company_price = None
        for company in company_prices:
            if company['ticker'] == ticker:
                company_price = company
                break

        # Determine recommendation
        if (company_price is None or recommend_dict['predicted_tomorrow_price'] is None or company_price['Close'] is None):
            if sentiment_score is None:
                recommend_dict['stock_recommendation'] = None
            elif sentiment_score >= 0:
                recommend_dict['stock_recommendation'] = 'BUY'
            elif sentiment_score < 0:
                recommend_dict['stock_recommendation'] = 'HOLD'
        elif sentiment_score is None:
            if recommend_dict['predicted_tomorrow_price'] > company_price['Close']:
                recommend_dict['stock_recommendation'] = 'BUY'
            else:
                recommend_dict['stock_recommendation'] = 'HOLD'
        elif recommend_dict['predicted_tomorrow_price'] >= company_price['Close'] and sentiment_score >= 0:
            recommend_dict['stock_recommendation'] = 'BUY'
        elif recommend_dict['predicted_tomorrow_price'] < company_price['Close'] and sentiment_score < 0:
            recommend_dict['stock_recommendation'] = 'SELL'
        elif recommend_dict['predicted_tomorrow_price'] <= company_price['Close'] and sentiment_score >= 0:
            recommend_dict['stock_recommendation'] = 'HOLD'

        recommendations.append(recommend_dict)
    
    test_recommendations(recommendations)

    if len(recommendations) > 0:
        db.insert_many_for_collection(database_name=DATABASE_NAME, collection_name=COMPANY_RECOMMENDATION, records=recommendations)


######################################## PREPARE TO LOAD TO BIGQUERY #######################################

def prepare_collections(**kwargs):
    db = DBHelper()

    selected_companies=kwargs['ti'].xcom_pull(key='company_names', task_ids='random_select_companies')

    combined_sentiments = db.get_documents_for_collection(DATABASE_NAME, COMBINED_SENTIMENTS)
    sm_sentiments = db.get_documents_for_collection(DATABASE_NAME, SOCIAL_MEDIA_SENTIMENTS)
    news_sentiments = db.get_documents_for_collection(DATABASE_NAME, NEWS_SENTIMENTS)
    recommendations = db.get_documents_for_collection(DATABASE_NAME, COMPANY_RECOMMENDATION)
    tech_indicators = db.get_documents_for_collection(DATABASE_NAME, COMPANY_TECH_INDICATORS)
    company_prices = db.get_documents_for_collection(DATABASE_NAME, COMPANY_PRICES)
    company_ratios = db.get_documents_for_collection(DATABASE_NAME, COMPANY_RATIOS)
    
    extract_company = []
    extract_social_media_news_sentiment_score = []
    extract_company_interest = []
    extract_tech_indicators =[]
    extract_company_prices = []

    for company in selected_companies:
        combined_sentiment = [x for x in combined_sentiments if x['company'] == company][0]
        recommendation = [x for x in recommendations if x['company'] == company][0]
        sm_sentiment = [x for x in sm_sentiments if x['company'] == company][0]
        news_sentiment = [x for x in news_sentiments if x['company'] == company][0]
        tech_indicator = [x for x in tech_indicators if x['company'] == company][0]
        company_price = [x for x in company_prices if x['company'] == company][0]
        company_ratio = [x for x in company_ratios if x['company'] == company][0]
        
        extract_company.append({
            'company_name': company,
            'ticker': recommendation['ticker'],
            'social_media_news_sentiment_score': combined_sentiment['compound_combined_sentiments'],
            'company_interest': combined_sentiment['combined_trend'],
            'company_sentiment_score': combined_sentiment['public_sentiment_score'],
            'stock_recommendation': recommendation['stock_recommendation'],
            'predicted_tomorrow_price': recommendation['predicted_tomorrow_price'],
            'EBITDA': company_ratio['EBITDA'] if company_ratio['EBITDA'] != np.nan else company_ratio['EBITDA'],
            'market_cap': company_ratio['Market Cap (intraday)'] if company_ratio['Market Cap (intraday)'] != np.nan else company_ratio['Market Cap (intraday)'],
            'enterprise_val': company_ratio['Enterprise Value'] if company_ratio['Enterprise Value'] != np.nan else company_ratio['Enterprise Value'],
            'trailing_pe': company_ratio['Trailing P/E'] if company_ratio['Trailing P/E'] != np.nan else company_ratio['Trailing P/E'],
            'forward_pe': company_ratio['Forward P/E'] if company_ratio['Forward P/E'] != np.nan else company_ratio['Forward P/E'],
            'peg_ratio': company_ratio['PEG Ratio (5 yr expected)'] if company_ratio['PEG Ratio (5 yr expected)'] != np.nan else company_ratio['PEG Ratio (5 yr expected)'],
            'price_to_sales': company_ratio['Price/Sales (ttm)'] if company_ratio['Price/Sales (ttm)'] != np.nan else company_ratio['Price/Sales (ttm)'],
            'price_to_book': company_ratio['Price/Book (mrq)'] if company_ratio['Price/Book (mrq)'] != np.nan else company_ratio['Price/Book (mrq)'],
            'enterprise_val_to_rev': company_ratio['Enterprise Value/Revenue'] if company_ratio['Enterprise Value/Revenue'] != np.nan else company_ratio['Enterprise Value/Revenue'],
            'enterprise_val_to_ebitda': company_ratio['Enterprise Value/EBITDA'] if company_ratio['Enterprise Value/EBITDA'] != np.nan else company_ratio['Enterprise Value/EBITDA'],
            'beta': company_ratio['Beta (5Y Monthly)'] if company_ratio['Beta (5Y Monthly)'] != np.nan else company_ratio['Beta (5Y Monthly)'],
            'week_52_high': company_ratio['52 Week High 3'] if company_ratio['52 Week High 3'] != np.nan else company_ratio['52 Week High 3'],
            'payout_ratio': company_ratio['Payout Ratio 4'] if company_ratio['Payout Ratio 4'] != np.nan else company_ratio['Payout Ratio 4'],
            'profit_margin': company_ratio['Profit Margin'] if company_ratio['Profit Margin'] != np.nan else company_ratio['Profit Margin'],
            'roa': company_ratio['Return on Assets (ttm)'] if company_ratio['Return on Assets (ttm)'] != np.nan else company_ratio['Return on Assets (ttm)'],
            'roe': company_ratio['Return on Equity (ttm)'] if company_ratio['Return on Equity (ttm)'] != np.nan else company_ratio['Return on Equity (ttm)'],
            'gross_profit': company_ratio['Gross Profit (ttm)'] if company_ratio['Gross Profit (ttm)'] != np.nan else company_ratio['Gross Profit (ttm)'],
            'diluted_eps': company_ratio['Diluted EPS (ttm)'] if company_ratio['Diluted EPS (ttm)'] != np.nan else company_ratio['Diluted EPS (ttm)'],
            'quarter_earnings_growth': company_ratio['Quarterly Earnings Growth (yoy)'] if company_ratio['Quarterly Earnings Growth (yoy)'] != np.nan else company_ratio['Quarterly Earnings Growth (yoy)'],
            'debt_to_equity': company_ratio['Total Debt/Equity (mrq)'] if company_ratio['Total Debt/Equity (mrq)'] != np.nan else company_ratio['Total Debt/Equity (mrq)'],
            'current_ratio': company_ratio['Current Ratio (mrq)'] if company_ratio['Current Ratio (mrq)'] != np.nan else company_ratio['Current Ratio (mrq)'],
            'book_val_per_share': company_ratio['Book Value Per Share (mrq)'] if company_ratio['Book Value Per Share (mrq)'] != np.nan else company_ratio['Book Value Per Share (mrq)'],
            'operating_cash_flow': company_ratio['Operating Cash Flow (ttm)'] if company_ratio['Operating Cash Flow (ttm)'] != np.nan else company_ratio['Operating Cash Flow (ttm)'],
            'timestamp': datetime.today().strftime('%Y-%m-%d')
        })

        extract_social_media_news_sentiment_score.append({
            'company_name': company,
            'number_relevant_tweets': sm_sentiment['number_relevant_tweets'],
            'average_twitter_sentiment_score': sm_sentiment['twitter_compound_sm_sentiments'],
            'number_telegram_messages': sm_sentiment['number_telegram_messages'],
            'average_telegram_sentiment_score': sm_sentiment['telegram_compound_sm_sentiments'],
            'number_relevant_news': news_sentiment['number_relevant_news'],
            'average_news_sentiments': news_sentiment['compound_news_sentiments'],
            'timestamp': datetime.today().strftime('%Y-%m-%d')
        })

        extract_company_interest.append({
            'company_name': company,
            'google_search_frequency_interest': combined_sentiment['google_search_frequency_interest'],
            'wiki_search_frequency': combined_sentiment['wiki_search_frequency'],
            'timestamp': datetime.today().strftime('%Y-%m-%d')
        })
        
        extract_tech_indicators.append({
            'company_name': company,
            'ticker': recommendation['ticker'],
            'timestamp': datetime.today().strftime('%Y-%m-%d'),
            'day_50_sma': tech_indicator['Day_50_SMA'] if tech_indicator['Day_50_SMA'] != np.nan else tech_indicator['Day_50_SMA'],
            'day_50_ema': tech_indicator['Day_50_EMA'] if tech_indicator['Day_50_EMA'] != np.nan else tech_indicator['Day_50_EMA'],
            'day_50_wma': tech_indicator['Day_50_WMA'] if tech_indicator['Day_50_WMA'] != np.nan else tech_indicator['Day_50_WMA'],
            'upper_band': tech_indicator['upper_band'] if tech_indicator['upper_band'] != np.nan else tech_indicator['upper_band'],
            'middle_band': tech_indicator['middle_band'] if tech_indicator['middle_band'] != np.nan else tech_indicator['middle_band'],
            'lower_band': tech_indicator['lower_band'] if tech_indicator['lower_band'] != np.nan else tech_indicator['lower_band'],
            'sar': tech_indicator['SAR'] if tech_indicator['SAR'] != np.nan else tech_indicator['SAR'],
            'macd': tech_indicator['MACD'] if tech_indicator['MACD'] != np.nan else tech_indicator['MACD'],
            'macd_signal': tech_indicator['MACD_signal'] if tech_indicator['MACD_signal'] != np.nan else tech_indicator['MACD_signal'],
            'macd_hist': tech_indicator['MACD_hist'] if tech_indicator['MACD_hist'] != np.nan else tech_indicator['MACD_hist'],
            'rsi': tech_indicator['RSI'] if tech_indicator['RSI'] != np.nan else tech_indicator['RSI'],
            'aroon_down': tech_indicator['AROON_down'] if tech_indicator['AROON_down'] != np.nan else tech_indicator['AROON_down'],
            'aroon_up': tech_indicator['AROON_up'] if tech_indicator['AROON_up'] != np.nan else tech_indicator['AROON_up'],
            'ppo': tech_indicator['PPO'] if tech_indicator['PPO'] != np.nan else tech_indicator['PPO'],
            'adx': tech_indicator['ADX'] if tech_indicator['ADX'] != np.nan else tech_indicator['ADX'],
            'cci': tech_indicator['CCI'] if tech_indicator['CCI'] != np.nan else tech_indicator['CCI'],
            'obv': tech_indicator['OBV'] if tech_indicator['OBV'] != np.nan else tech_indicator['OBV'],
            'atr': tech_indicator['ATR'] if tech_indicator['ATR'] != np.nan else tech_indicator['ATR']
        })
        
        extract_company_prices.append({
            'company_name': company,
            'ticker': recommendation['ticker'],
            'timestamp': datetime.today().strftime('%Y-%m-%d'),
            'open': company_price['Open'] if company_price['Open'] != np.nan else company_price['Open'],
            'high': company_price['High'] if company_price['High'] != np.nan else company_price['High'],
            'low': company_price['Low'] if company_price['Low'] != np.nan else company_price['Low'],
            'close': company_price['Close'] if company_price['Close'] != np.nan else company_price['Close'],
            'adj_close': company_price['Adj Close'] if company_price['Adj Close'] != np.nan else company_price['Adj Close'],
            'volume': company_price['Volume'] if company_price['Volume'] != np.nan else company_price['Volume']
        })

    if len(extract_company) > 0:
        db.insert_many_for_collection(DATABASE_NAME, EXTRACT_COMPANY_ANALYSIS, extract_company)
    if len(extract_social_media_news_sentiment_score) > 0:
        db.insert_many_for_collection(DATABASE_NAME, EXTRACT_SOCIAL_MEDIA_NEWS_SENTIMENT, extract_social_media_news_sentiment_score)
    if len(extract_company_interest) > 0:
        db.insert_many_for_collection(DATABASE_NAME, EXTRACT_COMPANY_INTEREST, extract_company_interest)
    if len(extract_company_prices) > 0:
        db.insert_many_for_collection(DATABASE_NAME, EXTRACT_COMPANY_PRICE, extract_company_prices)
    if len(extract_tech_indicators) > 0:
        db.insert_many_for_collection(DATABASE_NAME, EXTRACT_COMPANY_TECHNICAL_INDICATORS, extract_tech_indicators)
    
########################################### LOAD TO BIGQUERY ###########################################
def extract_collections_to_json():
    db = DBHelper()
    collections_to_extract = [EXTRACT_COMPANY_ANALYSIS, EXTRACT_COMPANY_TECHNICAL_INDICATORS, EXTRACT_COMPANY_INTEREST, EXTRACT_COMPANY_PRICE, EXTRACT_SOCIAL_MEDIA_NEWS_SENTIMENT]
    
    for collection in collections_to_extract:
        db.export_collection_to_json(DATABASE_NAME, collection)
        
########################################### CLEAN UP ###########################################

# clear all relevant temporary storage after data loaded to warehouse
def clean_up_database():
    db = DBHelper()
    db.clean_database(DATABASE_NAME)

############################################# UNIT TESTS ###########################################

def test_select_random_companies(selected_companies):
    assert type(selected_companies) == list
    assert len(selected_companies) == 10
    assert type(selected_companies[0]) == str

def test_get_news(response):
    assert type(response) == dict
    assert 'articles' in response
    assert 'company' in response

def test_get_media(response):
    assert type(response) == dict
    assert 'company' in response
    assert 'data' in response

def test_standardise_news(all_company_news):
    assert type(all_company_news) == list
    if len(all_company_news) > 0:
        assert 'company' in all_company_news[0]
        assert 'news' in all_company_news[0]

def test_calculate_news_sentiments(company_sentiments):
    assert type(company_sentiments) == list
    if len(company_sentiments) > 0:
        assert "neg_news_sentiments" in company_sentiments[0]
        assert "pos_news_sentiments" in company_sentiments[0]
        assert "neu_news_sentiments" in company_sentiments[0] 
        assert "compound_news_sentiments" in company_sentiments[0] 
        assert "number_relevant_news" in company_sentiments[0] 

def test_social_media_sentiments(sentiments):
    assert type(sentiments) == list
    if len(sentiments) > 0:
        assert "company" in sentiments[0]
        assert "telegram_compound_sm_sentiments" in sentiments[0]
        assert "twitter_compound_sm_sentiments" in sentiments[0]
        assert "pos_sm_sentiments" in sentiments[0]
        assert "neu_sm_sentiments" in sentiments[0]
        assert "neg_sm_sentiments" in sentiments[0]
        assert "compound_sm_sentiments" in sentiments[0]
        assert "number_relevant_tweets" in sentiments[0]
        assert "number_telegram_messages" in sentiments[0]

def test_google_trends(trends):
    assert type(trends) == list
    if len(trends) > 0:
        assert "company" in trends[0]
        assert "daily_interests" in trends[0]
        assert "average" in trends[0]

def test_company_wiki_trends(trends):
    assert type(trends) == list
    if len(trends) > 0:
        assert "company" in trends[0]
        assert "number_views" in trends[0]

def test_get_tweets(tweets):
    assert type(tweets) == list
    if len(tweets) > 0:
        assert "company" in tweets[0]
        assert "content" in tweets[0]
        assert "data" in tweets[0]
        assert "source" in tweets[0]

def test_telegram_messages(messages):
    assert type(messages) == list
    if len(messages) > 0:
        assert "company" in messages[0]
        assert "content" in messages[0]
        assert "data" in messages[0]
        assert "source" in messages[0]

def test_combine_sentiments(weighted_score):
    assert type(weighted_score) == list
    if len(weighted_score) > 0:
        assert "company" in weighted_score[0]
        assert "compound_combined_sentiments" in weighted_score[0]
        assert "combined_trend" in weighted_score[0]
        assert "google_search_frequency_interest" in weighted_score[0]
        assert "wiki_search_frequency" in weighted_score[0]
        assert "public_sentiment_score" in weighted_score[0]
        assert "number_relevant_tweets" in weighted_score[0]
        assert "number_telegram_messages" in weighted_score[0]
    

def test_recommendations(recommendations):
    assert type(recommendations) == list
    if len(recommendations) > 0:
        assert "ticker" in recommendations[0]
        assert "company" in recommendations[0]
        assert "predicted_tomorrow_price" in recommedations[0]
        assert "stock_recommendation" in recommendations[0]

############################################# DAG ###########################################

default_args = {
    'owner': 'kiyong',
    'depends_on_past': False,
    'email': ['angkiyong@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

company_fundamental_analysis_dag = DAG(
    'company_fundamental_analysis',
    default_args=default_args,
    description='news',
    schedule_interval="@daily",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['news']
)

################################## EXTRACT ##################################
select_random_companies = PythonOperator(
    task_id='random_select_companies',
    python_callable=random_select_companies,
    dag=company_fundamental_analysis_dag
)

retrieve_newsapi_api_2 = PythonOperator(
    task_id='retrieve_newsapi_api_2',
    python_callable=get_newsapi_api,
    dag=company_fundamental_analysis_dag
)

retrieve_mediastack_api_2 = PythonOperator(
    task_id='retrieve_mediastack_api_2',
    python_callable=get_mediastack_api,
    dag=company_fundamental_analysis_dag
)

retrieve_tweets = PythonOperator(
    task_id='retrieve_tweets',
    python_callable=get_tweets,
    dag=company_fundamental_analysis_dag
)

retrieve_telegram_messages = PythonOperator(
    task_id='retrieve_telegram_messages',
    python_callable=get_telegram_messages,
    dag=company_fundamental_analysis_dag
)

retrieve_company_trends = PythonOperator(
    task_id='retrieve_company_trends',
    python_callable=get_google_trends,
    dag=company_fundamental_analysis_dag
)

retrieve_company_wiki_trends = PythonOperator(
    task_id='retrieve_company_wiki_trends',
    python_callable=get_company_wiki_trends,
    dag=company_fundamental_analysis_dag
)

retrieve_company_prices = PythonOperator(
    task_id='retrieve_company_prices',
    python_callable=get_company_prices,
    dag=company_fundamental_analysis_dag
)

retrieve_company_ratios = PythonOperator(
    task_id='retrieve_company_ratios',
    python_callable=get_company_ratios,
    dag=company_fundamental_analysis_dag
)

################################## PROCESS ##################################

standardise_news = PythonOperator(
    task_id='standardise_news',
    python_callable=standardise_news_data,
    dag=company_fundamental_analysis_dag
)

filter_raw_spams = PythonOperator(
    task_id='filter_raw_spams',
    python_callable=filter_all_spams,
    dag=company_fundamental_analysis_dag
)

calculate_social_media_sentiments = PythonOperator(
    task_id='calculate_social_media_sentiments',
    python_callable=social_media_sentiments,
    dag=company_fundamental_analysis_dag
)

calculate_news_sentiments = PythonOperator(
    task_id='calculate_news_sentiments',
    python_callable=news_sentiment_analysis,
    dag=company_fundamental_analysis_dag
)

combine_all_sentiments = PythonOperator(
    task_id='combine_all_sentiments',
    python_callable=combine_sentiments,
    dag=company_fundamental_analysis_dag
)

company_tech_indicators = PythonOperator(
    task_id='company_tech_indicators',
    python_callable=get_company_tech_indicators,
    dag=company_fundamental_analysis_dag
)

company_recommendations = PythonOperator(
    task_id='company_recommendations',
    python_callable=generate_recommendation,
    dag=company_fundamental_analysis_dag
)

################################# LOAD (UNCOMMENT IF USING) ##################################

prepare_collections_for_loading = PythonOperator(
    task_id='prepare_collections_for_loading',
    python_callable=prepare_collections,
    dag=company_fundamental_analysis_dag
)

extract_collections = PythonOperator(
    task_id='extract_collections',
    python_callable=extract_collections_to_json,
    dag=company_fundamental_analysis_dag
)

load_to_gcs = BashOperator(
    task_id='load_to_gcs', 
    bash_command="/home/airflow/3107-pipeline/upload/load_gcs.sh ",
    dag=company_fundamental_analysis_dag
)

load_to_bq = BashOperator(
    task_id='load_to_bq', 
    bash_command="/home/airflow/3107-pipeline/upload/load_bq.sh ",
    dag=company_fundamental_analysis_dag
)

################################## CLEAN UP (UNCOMMENT IF USING) ##################################

clean_database = PythonOperator(
    task_id='clean_database',
    python_callable=clean_up_database,
    dag=company_fundamental_analysis_dag
)

################################## GRAPH ##################################
select_random_companies >> [retrieve_mediastack_api_2, retrieve_newsapi_api_2] >> standardise_news >> calculate_news_sentiments
select_random_companies >> [retrieve_tweets, retrieve_telegram_messages] >> filter_raw_spams >> calculate_social_media_sentiments
select_random_companies >> [retrieve_company_trends, retrieve_company_wiki_trends]
[calculate_news_sentiments, calculate_social_media_sentiments, retrieve_company_trends, retrieve_company_wiki_trends] >> combine_all_sentiments
select_random_companies >> [retrieve_company_prices, retrieve_company_ratios, company_tech_indicators] >> company_recommendations
combine_all_sentiments >> company_recommendations >> prepare_collections_for_loading >> extract_collections >> load_to_gcs >> load_to_bq >> clean_database

################ TEST (DO NOT UNCOMMENT UNLESS USING)###################
# prepare_collections_for_loading >> extract_collections >> load_to_gcs >> load_to_bq >> clean_database OKAY
# select_random_companies >> [retrieve_company_ratios, retrieve_company_prices, company_tech_indicators] 
# select_random_companies >> [retrieve_company_trends, retrieve_company_wiki_trends] OKAY
# select_random_companies >> [retrieve_mediastack_api_2, retrieve_newsapi_api_2] >> standardise_news >> calculate_news_sentiments
# select_random_companies >> [retrieve_tweets, retrieve_telegram_messages] >> filter_raw_spams >> calculate_social_media_sentiments
# select_random_companies >> [combine_all_sentiments, calculate_social_media_sentiments]
# select_random_companies >> company_recommendations
