import datetime as dt
from keras.models import Sequential
from keras.layers import Dense, LSTM, Dropout
from sklearn.preprocessing import MinMaxScaler
import numpy as np
import pandas as pd
import yfinance as yf
import requests
import datetime as dt
from pytrends.request import TrendReq
import json
from math import sqrt
from sklearn.metrics import mean_squared_error

def predict_stock_price_for_tomorrow(stock, features=None, steps=1, training=0.9, period=14, years=1, error_method='mape', plot=False):
    """
    Function to predict the "close price" for the next day.

    Arguments:
        stock (str): stock label
        features (list): ['Interest', 'Wiki_views', 'RSI', '%K', '%R']
        steps (int): previous days to consider for generating the model.
        training (float): fraction assigned for training the model
        period (int): number of days considered for calculating indicators.
        years (int or float): years of data to be considered
        error_method (str): 'mape' or 'mse'
        plot (bool): generate performance plot

    Returns:
        Result for the next business day. [price, error, date]
    """

    if features is None:
        features = []

    # GET ALL THE DATA:
    stock_data = total(stock, years=years, interest='Interest' in features, wiki_views='Wiki_views' in features,
                       indicators='RSI' and '%K' and '%R' in features, period=period)

    removing = []
    for feature in features:
        if feature not in stock_data.columns:
            removing.append(feature)

    for ft in removing:
        features.remove(ft)

    # SPLIT DATA, CREATE THE MODEL, GENERATE AND CALCULATE THE ERROR:
    result, y_predicted, df = run(stock_data, features, steps, training, error_method)

    date = (dt.datetime.today() + dt.timedelta(days=1))
    while date.weekday() == 5 or date.weekday() == 6:
        date = date + dt.timedelta(days=1)
    date = date.strftime('%Y-%m-%d')
    result.append(date)

    return result[0]

def data(df, features=[]):
    columns = ['Close']
    if len(features) > 0:
        for i in range(len(features)):
            columns.append(features[i])

    df = df[columns]

    return df


def get_lstm_input(data, steps=1):
    samples = []
    for i in range(steps, data.shape[0]):
        features = []
        for j in range(steps):
            features.append(data[i - steps + j, :])
        features.append(data[i, :])
        samples.append(features)

    features = []
    for j in range(steps + 1):
        features.append(data[-1, :])

    samples.append(features)
    samples = np.asarray(samples)
    return samples


def run(df, features=[], steps=1, training=0.9, error_method='mape'):

    new_df = data(df, features)

    scaler = MinMaxScaler(feature_range=(0, 1))
    scaled = scaler.fit_transform(new_df)
    reframed = get_lstm_input(scaled, steps)

    rows = round(len(df) * training)

    train = reframed[:rows, :, :]
    test = reframed[rows:, :, :]

    train_x, train_y = train[:, :steps, :], train[:, steps, 0]
    test_x, test_y = test[:, :steps, :], test[:-1, steps, 0]

    # designing and fitting network
    model = Sequential()
    model.add(LSTM(50, input_shape=(train_x.shape[1], train_x.shape[2])))
    model.add(Dropout(0.2))
    model.add(Dense(1))
    model.compile(loss='mae', optimizer='adam')
    model.fit(train_x, train_y, epochs=100, batch_size=70, verbose=0)

    mod1 = rows + steps - 1
    mod2 = rows + steps

    # generate a prediction
    prediction = model.predict(test_x)
    new_scaled = np.copy(scaled)
    for x in range(mod1, new_scaled.shape[0]):
        new_scaled[x, 0] = prediction[x-mod1]

    # invert normalized values
    # for predictions
    y_predicted = scaler.inverse_transform(new_scaled)
    y_predicted = y_predicted[mod1:, 0]
    # for real values
    y = scaler.inverse_transform(scaled)
    y = y[mod2:, 0]

    finalprice = round(y_predicted[-1], 2)
    y_predicted = y_predicted[:-1]

    error = get(y, y_predicted, error_method)

    result = [finalprice, error]

    return result, y_predicted, new_df[-len(y):]

def main(stock, years=1):  # function to get data from Yahoo Finance
    end = dt.datetime.today().strftime('%Y-%m-%d')  # today as the end date
    start = (dt.datetime.today() - dt.timedelta(days=365*years)).strftime('%Y-%m-%d')  # 1 year ago as start
    df = yf.download(stock, start, end)

    return df, start, end


def company_name(stock):  # function to get the company's name from the stock
    url = f"http://d.yimg.com/autoc.finance.yahoo.com/autoc?query={stock}&region=1&lang=en" # source
    try:
        company = requests.get(url).json()['ResultSet']['Result'][0]['name']   # saving the name as 'company'
    except json.decoder.JSONDecodeError:
        return None

    return company


def get_interest(company, timeframe):  #  base function to get 'interest' from Google Trends
    (pytrend := TrendReq()).build_payload(kw_list=[company], timeframe=timeframe)  # accessing to Google Trends using pytrends package and finding interest for 'company' during 'timeframe'
    return (result := pytrend.interest_over_time().drop('isPartial', axis=1))  # saving the 'interest' values


def add_interest(df, company, years=1):  # main function to get 'interest' from Google Trends
    delta = int((365 * years / 73) - 1)  # dividing the year in groups of 73 days
    since = (dt.datetime.today() - dt.timedelta(days=365 * years)).strftime('%Y-%m-%d')
    until = (dt.datetime.today() - dt.timedelta(days=73 * delta)).strftime('%Y-%m-%d')
    timeframe = since + ' ' + until  # setting the required format
    trends = get_interest(company, timeframe)  # get the values for the first 73 days
    for x in range(delta):  # get the values for the rest of the year
        since = (dt.datetime.today() - dt.timedelta(days=73 * (delta - x))).strftime('%Y-%m-%d')
        until = (dt.datetime.today() - dt.timedelta(days=73 * (delta - 1 - x))).strftime('%Y-%m-%d')
        timeframe = since + ' ' + until
        trends.append(get_interest(company, timeframe))

    trends.rename(columns={company: 'Interest'}, inplace=True)  # changing title to 'Interest'
    trends.index.names = ['Date']
    return ((df := df.merge(trends, how='left', on='Date')).Interest.interpolate(inplace=True))  # Add Interest column from Google Trends API - pytrends and interpolation for missing values



def add_wiki_views(df, company, start, end):  # function to get number of page views from Wikipedia
    start = start.replace('-', '')
    end = end.replace('-', '')
    link = 'https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/en.wikipedia/all-access/all-agents' \
           '/{company}/daily/{st}/{end}'.format(company=company, st=start, end=end)
    r = requests.Session()
    r.headers = {"User-Agent": "stocker/0.1.7 (https://github.com/jcamiloangarita/stocker; camiloang94@gmail.com)"}
    response = r.get(link)
    wiki_data = response.json()  # get the data from Wikipedia API
    views = [i['views'] for i in wiki_data['items']]  # saving views values
    date = [i['timestamp'] for i in wiki_data['items']]  # saving dates
    date = [dt.datetime.strptime(date[:-2], '%Y%m%d').date().strftime('%Y-%m-%d') for date in date]  # change format
    wiki_views = pd.DataFrame(views, index=date, columns=['Wiki_views'])
    wiki_views.index.name = 'Date'
    wiki_views.index = pd.to_datetime(wiki_views.index)

    return ((df := df.merge(wiki_views, how='left', on='Date')).Wiki_views.ffill(inplace=True))  # Add Wiki_views column from Wikipedia API



def add_rsi(df, period):    # function to Calculate RSI values
    df['Change'] = df.Close - df.Open  # calculating gains and losses in a new column
    df['Gain'] = df.Change[df.Change > 0]  # new column of gains
    df['Loss'] = df.Change[df.Change < 0] * (-1)  # new column of losses
    df.drop(columns=['Change'], inplace=True)  # remove the column change

    # Filling missing values with 0
    df.Gain.fillna(0, inplace=True)
    df.Loss.fillna(0, inplace=True)

    df['Again'] = df.Gain.rolling(period).mean()  # calculate the average gain in the last 14 periods
    df['Aloss'] = df.Loss.rolling(period).mean()  # calculate the average loss in the last 14 periods

    df['RS'] = df.Again / df.Aloss  # calculating RS
    df['RSI'] = 100 - (100 / (1 + (df.Again / df.Aloss)))  # calculating RSI
    df.drop(columns=['Gain', 'Loss', 'Again', 'Aloss', 'RS'], inplace=True)  # remove undesired columns

    return df


def add_k(df, period):   # Calculate Stochastic Oscillator (%K)
    df['L14'] = df.Low.rolling(period).min()  # find the lowest price in the last 14 periods
    df['H14'] = df.High.rolling(period).max()  # find the highest price in the last 14 periods
    df['%K'] = ((df.Close - df.L14) / (df.H14 - df.L14)) * 100
    df.drop(columns=['L14', 'H14'], inplace=True)  # remove columns L14 and H14

    return df


def add_r(df, period):  # Calculate Larry William indicator (%R)
    df['HH'] = df.High.rolling(period).max()  # find the highest high price in the last 14 periods
    df['LL'] = df.Low.rolling(period).min()  # find the lowest low price in the last 14 periods
    df['%R'] = ((df.HH - df.Close) / (df.HH - df.LL)) * (-100)
    df.drop(columns=['HH', 'LL'], inplace=True)  # remove columns HH and LL

    return df


def total(stock, years=1, interest=False, wiki_views=False, indicators=False, period=14):
    # main function to combine data from Yahoo Finance, Google Trends, Wikipedia and calculated indicators.
    df, start, end = main(stock, years=years)  # get data from Yahoo Finance and define star and end
    company = None
    if interest or wiki_views:
        company = company_name(stock)  # get the name of the company

    if company is not None:
        if interest:
            df = add_interest(df, company, years=years)  # adding Interest from Google Trends.
        if wiki_views:
            df = add_wiki_views(df, company, start, end)  # adding Wiki Views

    if indicators:  # Adding indicators
        df = add_k(df, period)  # generating %K column.
        df = add_r(df, period)  # generating %R column.
        df = add_rsi(df, period)  # generating RSI column.

    return (df := df.dropna())   # drop rows with missing data



def correlation(stock, years=1, interest=False, wiki_views=False, indicators=False, period=14, complete=True, limit=0.5):
    # function to get the Pearson correlation coefficients for all the features

    df = total(stock, years, interest, wiki_views, indicators, period)

    if complete:
        features = df.corr().Close
    else:  # only the coefficients against the close prices
        features = df.corr().Close[df.corr().Close > limit].index.tolist()

    return features

def get(true_values, predicted_values, error_method='mape'):    # function to calculate the error

    if error_method == 'mape':
        # calculate the mean absolute percentage error
        return round((abs((true_values - predicted_values) / true_values).sum() / len(true_values)) * 100, 3)

    elif error_method == 'mse':
        # calculate the mean squared error
        return round(sqrt(mean_squared_error(true_values, predicted_values)), 3)
        
    else:
        raise ValueError('This error method is not supported')
