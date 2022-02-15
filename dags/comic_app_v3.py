import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from airflow.operators.dummy import DummyOperator
# from airflow.providers.slack.hooks.slack_webhook import SlackWebhookOperator
# from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from settings import SLACK_WEBHOOK_TOKEN



# Slack Webhook Resources:
# https://stackoverflow.com/questions/52054427/how-to-integrate-apache-airflow-with-slack

# 這才是最新的 webhook class
# https://github.com/apache/airflow/blob/main/airflow/providers/slack/operators/slack_webhook.py 

# 已棄用
# https://github.com/apache/airflow/blob/1801baefe44f361010c23e6ec4ee8b8569eab82d/airflow/contrib/operators/slack_webhook_operator.py#L25
# https://github.com/apache/airflow/blob/main/airflow/contrib/operators/slack_webhook_operator.py#L25

# 要去 slack 申請 app 然後拿 incoming webhook 的 token
# https://api.slack.com/apps/A030B9657EH/incoming-webhooks?

default_args = {
    'owner': 'phil',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def process_metadata(mode, **context):
    if mode == 'read':
        print("取得使用者的閱讀紀錄")
    elif mode == 'write':
        print("更新閱讀紀錄")


def check_comic_info(**context):
    all_comic_info = context['task_instance'].xcom_pull(task_ids='get_read_history')
    print("去漫畫網站看有沒有新的章節")

    anything_new = time.time() % 2 > 1
    return anything_new, all_comic_info


def decide_what_to_do(**context):
    anything_new, all_comic_info = context['task_instance'].xcom_pull(task_ids='check_comic_info')

    print("跟紀錄比較，有沒有新連載？")
    if anything_new:
        return 'yes_generate_notification'
    else:
        return 'no_do_nothing'


def generate_message(**context):
    _, all_comic_info = context['task_instance'].xcom_pull(task_ids='check_comic_info')
    print("產生要寄給 Slack 的訊息內容並存成檔案")


with DAG(
    dag_id = 'comic_app_v3', 
    default_args=default_args,
    description='This is comic_app_v3.',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1, 0, 0),
    catchup=False,
    tags=['phil'],
    ) as dag:

    get_read_history = PythonOperator(
        task_id='get_read_history',
        python_callable=process_metadata,
        op_args=['read']
    )

    check_comic_info = PythonOperator(
        task_id='check_comic_info',
        python_callable=check_comic_info,
        # provide_context=True
    )

    decide_what_to_do = BranchPythonOperator(
        task_id='new_comic_available',
        python_callable=decide_what_to_do,
        # provide_context=True
    )

    update_read_history = PythonOperator(
        task_id='update_read_history',
        python_callable=process_metadata,
        op_args=['write'],
        # provide_context=True
    )

    generate_notification = PythonOperator(
        task_id='yes_generate_notification',
        python_callable=generate_message,
        # provide_context=True
    )

    # {{ ds }} 為 jinja 用法: yyyy-mm-DD
    # {{ yesterday_ds }}: 限定特定日期
    # {{ tomorrow_ds }} 用法: yyyy-mm-DD
    send_notification = SlackWebhookOperator(
        task_id='send_notification',
        http_conn_id='send_notification',
        webhook_token=SLACK_WEBHOOK_TOKEN,
        channel='#airflow-testing',
        message=':pc: [{{ ds }}] This is a webhook from airflow, success!',
        link_names=False,
        username='phil',
        # icon_url='https://emojis.slackmojis.com/emojis/images/1620282616/36373/pc.gif?1620282616'
    )

    do_nothing = DummyOperator(task_id='no_do_nothing')

    # define workflow
    get_read_history >> check_comic_info >> decide_what_to_do
    decide_what_to_do >> generate_notification
    decide_what_to_do >> do_nothing
    generate_notification >> send_notification >> update_read_history