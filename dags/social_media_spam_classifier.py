import os.path
import scipy.io as sio
import numpy as np
import re
import nltk
import os
import numpy as np

from os.path import join, dirname
from typing import List, Tuple
from sklearn import svm
from sklearn.svm import LinearSVC
from typing import Dict, List, Tuple

# Support Vector Machine
linear_svm = svm.SVC(C=0.1, kernel="linear")
linear_svc = LinearSVC()

def train_svm():
    dataset = sio.loadmat("/home/airflow/3107-pipeline/static/spamTrain.mat")
    X, y = dataset["X"], dataset["y"]
    linear_svm.fit(X, y.flatten())

def classify_spam(content) -> int:
    train_svm()
    vocablary_dict = get_vocablary_dict()
    feature_vector = feature_vector_content(content, vocablary_dict)
    double_dimesion_content = np.reshape(feature_vector, (-1, 1899))
    spam_prediction = linear_svm.predict(double_dimesion_content)
    return spam_prediction

def preprocess(content) -> str:
    content = content.lower()
    content = re.sub("<[^<>]+>", " ", content)
    content = re.sub("[0-9]+", "number", content)
    content = re.sub(r"(http|https)://[^\s]*", "httpaddr", content)
    content = re.sub("[$]+", "dollar", content)
    return content

def create_tokenlist(content) -> List:
    stemmer = nltk.stem.porter.PorterStemmer()
    content = preprocess(content)
    tokens = re.split(r"[ \@\$\/\#\.\-\:\&\*\+\=\[\]\?\!\(\)\{\}\,\'\"\>\_\<\;\%]", content)
    tokenlist = []
    for token in tokens:
        token = re.sub("[^a-zA-Z0-9]", "", token)
        stemmed = stemmer.stem(token)
        if not len(token):
            continue
        tokenlist.append(stemmed)
    return tokenlist

def get_vocablary_dict() -> Dict:
    vocablary_dict = {}
    with open("/home/airflow/3107-pipeline/static/vocablary.txt", "r") as f:
        for line in f:
            (val, key) = line.split()
            vocablary_dict[int(val)] = key
    return vocablary_dict

def get_vocablary_indices(content, vocablary_dict: Dict) -> List:
    tokenlist = create_tokenlist(content)
    index_list = [
        vocablary_dict[token] for token in tokenlist if token in vocablary_dict
    ]
    return index_list

def feature_vector_content(content, vocablary_dict: Dict) -> Dict:
    n = len(vocablary_dict)
    result = np.zeros((n, 1))
    vocablary_indices = get_vocablary_indices(content, vocablary_dict)
    for index in vocablary_indices:
        result[index] = 1
    return result