# IS3107

This project proposes a combined proof of concept data pipeline that handles both traditional and technical analysis of Singapore stock market data (SGX) by pulling market data such as company performance and financial ratios as well as sentiment data from social media and news outlets.

The pipelines automatically process the data to churn out company recommendations based on qualitative and quantitative metrics, as well as forecasting of company prices and performance through the use of machine learning models such as FinBERT Natural Language Processing and Long Short-Term Memory Neural Networks.

The pipeline requires Apache Airflow, and the following python packages. Detailed packages are listed in requirements.txt.

There are two main pipelines, one pipeline for company fundamental analysis (qualitative and quantitative), and a pipeline for sector level analysis.

# Installation guide for packages

## NEEDED: Install mtools

Install mtools [http://blog.rueckstiess.com/mtools/mlaunch.html]. mtools is a compiled list of scripts useful for managing local mongoDB.

- Refer to docs [http://blog.rueckstiess.com/mtools/mlaunch.html]

Transactions in mongoDB are only allowed on a replica set members. To make life easier, we will use mlaunch to start the required instances.

1. Go to 3107-pipeline folder. Run `mlaunch --replicaset`
2. Type `mlaunch list`, you should see the following, with the following port numbers.

```
    PROCESS    PORT     STATUS     PID

    mongod     27017    running    3635
    mongod     27018    running    3710
    mongod     27019    running    3781
```

3. It will generate a data folder in the dir where you ran the command. The data folder contains all the data for the mongoDB instances e.g. indexes, collections, change logs. You should see data > replset > [rs1, rs2, rs3]
4. Subsequently, you can go to the dir where the data folder is and type `mlaunch start` to start the local mongod instance to run replica sets

## NEEDED: Directories

1. Custom python files created should be placed in the `3107-pipeline/dags` dir to avoid importing issues in DAG files.

## NEEDED: Installing gcloud CLI

Follow: https://cloud.google.com/sdk/docs/install#deb

```
sudo apt-get install apt-transport-https ca-certificates gnupg

echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | sudo tee -a /etc/apt/sources.list.d/google-cloud-sdk.list

curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key --keyring /usr/share/keyrings/cloud.google.gpg add -

sudo apt-get update && sudo apt-get install google-cloud-cli

gcloud init --console-only
```

Project ID

```
project-346314
```

GCS bucket

```
gs://3107-cloud-storage/
```

## NEEDED: Installing jq

```
sudo apt  install jq
```

## NEEDED: Installing ta-lib

1. follow this link https://mrjbq7.github.io/ta-lib/install.html

## NEEDED: NLTK

1. add the following in your bashrc and run source to update

```
export NLTK_DATA="/home/airflow/nltk_data"
```

## NEEDED: PyTorch

Follow: https://pytorch.org/get-started/locally/
