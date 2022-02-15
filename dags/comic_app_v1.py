import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    'owner': 'phil',
    'email': ['linooohon@gmail.com'],
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}


# default_args = {
#     'owner': 'phil',
#     'depends_on_past': False,
#     'email': ['linooohon@gmail.com'],
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }

def fn_superman():
    print("取得使用者的閱讀紀錄")
    print("去漫畫網站看有沒有新的章節")
    print("跟紀錄比較，有沒有新連載？")

    # Murphy's Law
    accident_occur = time.time() % 2 > 1
    if accident_occur:
        print("\n天有不測風雲,人有旦夕禍福")
        print("工作遇到預期外狀況被中斷\n")
        return

    new_comic_available = time.time() % 2 > 1
    if new_comic_available:
        print("寄 Slack 通知")
        print("更新閱讀紀錄")
    else:
        print("什麼都不幹，工作順利結束")


with DAG(
    'comic_app_v1', 
    default_args=default_args,
    description='comic_app_v1 description',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    # catchup=False,
    tags=['phil'],
) as dag:

    superman_task = PythonOperator(
        task_id='superman_task',
        python_callable=fn_superman
    )