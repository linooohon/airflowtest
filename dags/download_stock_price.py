import os
from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import yfinance as yf
from airflow.models import Variable

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'phil',
    'depends_on_past': False,
    'email': ['linooohon@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def download_prices(**context):
    stock_list_dic = Variable.get("stock_list_json", deserialize_json=True)
    
    stocks = context["dag_run"].conf.get("stocks")
    print(stocks)
    if stocks:
        stock_list_dic = stocks

    for ticker in stock_list_dic:
        # ticker = "MSFT" # Microsoft
        msft = yf.Ticker(ticker)
        hist = msft.history(period="max")
        # print(type(hist))
        # print(hist.shape)
        # print(hist)

        execute_file_dir_path = os.path.dirname(os.path.realpath(__file__))
        print(execute_file_dir_path)
        parentdir_path = os.path.dirname(execute_file_dir_path)
        print(parentdir_path)
        with open(f"{parentdir_path}/logs/{ticker}.csv", "w") as writer:
            hist.to_csv(writer, index=True)
        print("Finished downloading price data." + ticker)


# def download_prices():
#     stock_list_str = Variable.get("stock_list")
#     stock_list_json = Variable.get("stock_list_json", deserialize_json=True)
#     print(stock_list_str)
#     print(stock_list_json)
#     print(len(stock_list_json))
#     print(stock_list_json[:2])


# [START instantiate_dag]
with DAG(
    dag_id = 'Download_Stock_Price',
    default_args=default_args,
    description='This DAG downloads stocks prices and save them into text files.',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['phil'],
) as dag:
    dag.doc_md = """
    This is a dag for downloading
    """ 
    download_task = PythonOperator(
        task_id = "download_prices",
        python_callable = download_prices,
        provide_context = True # trigger時會帶參數
    )