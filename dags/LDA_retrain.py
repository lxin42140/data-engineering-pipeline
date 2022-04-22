import boto3
from airflow.operators.email_operator import EmailOperator
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from gensim.parsing.preprocessing import remove_stopwords
from gensim.models.coherencemodel import CoherenceModel
from gensim.models.ldamulticore import LdaMulticore
from gensim.models import CoherenceModel
import pandas as pd
import numpy as np
import gensim.corpora as corpora
import gensim
import datetime as dt
import re
import string
import pickle
import nltk
import io
from io import StringIO
nltk.download('wordnet')

############################################# CONSTANTS ###########################################

AWSREGION = 'ap-southeast-1'
AWSKEY = 'AKIAXL3EKMZMGCYQJBOJ'
AWSSECRETKEY = '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'
EMAIL = 'lwenqi.wql@gmail.com'

############################################# LDA retrain ###########################################


def lda_dataprep(df):
    df = df.copy()
    # remove non-ascii characters
    df['content'] = df.content.apply(lambda x: x.encode('utf8'))
    df['content'] = df.content.apply(
        lambda x: re.sub(rb'[^\x00-\x7f]', rb' ', x))

    # lower case
    df['content'] = df['content'].astype(str)
    df['content'] = df.content.apply(lambda x: x.lower())

    # remove punctuation
    punct = set(string.punctuation)
    df['content'] = df.content.apply(lambda x: x.translate(
        str.maketrans(string.punctuation, ' '*len(string.punctuation))))

    # tokenize
    df['content'] = df.content.apply(lambda x: x.split())

    # remove integers
    df['content'] = df.content.apply(
        lambda x: [w for w in x if not w.isnumeric()])

    # remove stopwords
    df['content'] = df.content.apply(
        lambda x: [remove_stopwords(w) for w in x])

    # lemmatize
    wordnet_lemmatizer = nltk.WordNetLemmatizer()
    df['content'] = df.content.apply(
        lambda x: wordnet_lemmatizer.lemmatize(' '.join(x)))

    # remove single letter
    lst = []
    for content in df['content']:
        newstr = ''
        contentsplit = content.split(' ')
        for word in contentsplit:
            if len(word) > 1:
                newstr += ' ' + word
        lst.append(newstr)
    df['content'] = lst

    return df


def retrain_lda():
    # get data
    s3 = boto3.resource(
        service_name='s3',
        region_name=AWSREGION,
        aws_access_key_id=AWSKEY,
        aws_secret_access_key=AWSSECRETKEY
    )
    dfobj = s3.Object('is3107-models', 'lda_train_data.csv')
    df = pd.read_csv(io.BytesIO(dfobj.get()['Body'].read()))

    # data formatting
    df = lda_dataprep(df)

    # get corpus
    text_data = df.content.apply(lambda x: [w for w in x.split()])
    dictionary = corpora.Dictionary(text_data)
    corpus = [dictionary.doc2bow(text) for text in text_data]

    perp_values = []
    coh_values = []
    model_list = {}
    output = ''
    for num_topics in range(5, 30, 5):
        model = LdaMulticore(corpus, id2word=dictionary,
                             num_topics=num_topics, workers=5)
        perplexity = model.log_perplexity(corpus)
        model_list['topic_' + str(num_topics)] = model
        perp_values.append(perplexity)
        cm = CoherenceModel(model=model, corpus=corpus, coherence='u_mass')
        coh_values.append(cm.get_coherence())
        output += '\nTopic: {}, Perplexity: {}\n'.format(
            num_topics, perplexity)
        output += 'Topic: {}, Coherence: {}\n'.format(
            num_topics, cm.get_coherence())
        output += str(model.print_topics())

    # need to push models to s3
    newmodelspkl = pickle.dumps(model_list)
    newdictpkl = pickle.dumps(dictionary)
    newoutputpkl = pickle.dumps(output)
    newmodelsobj = s3.Object('is3107-models',
                             dt.datetime.today().strftime("%Y%m%d") + '_lda_models' + '.pkl')
    newdictobj = s3.Object('is3107-models',
                           dt.datetime.today().strftime("%Y%m%d") + '_dict' + '.pkl')
    newoutputobj = s3.Object('is3107-models',
                             dt.datetime.today().strftime("%Y%m%d") + '_train_results' + '.pkl')
    newmodelsobj.put(Body=newmodelspkl)
    newdictobj.put(Body=newdictpkl)
    newoutputobj.put(Body=newoutputpkl)


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
retrain_lda_dag = DAG(
    'retrain_lda',
    description='Retrain LDA model',
    schedule_interval=dt.timedelta(days=30),
    start_date=dt.datetime(2021, 1, 1),
    catchup=False,
    tags=['industry', 'lda'],
)

############################################# TASKS ###########################################

retrain_lda_task = PythonOperator(
    task_id='retrain_lda',
    python_callable=retrain_lda,
    dag=retrain_lda_dag
)

notify_user_task = EmailOperator(
    task_id='notify_user',
    to=EMAIL,
    subject='LDA model training complete',
    html_content=f'LDA Model retrained on: {dt.datetime.today().strftime("%Y-%m-%d")}, access the outputs on your S3 console',
    dag=retrain_lda_dag
)

retrain_lda_task >> notify_user_task
