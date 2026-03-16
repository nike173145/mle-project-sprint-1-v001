import os
from dotenv import load_dotenv
from airflow.providers.telegram.hooks.telegram import TelegramHook

load_dotenv("/home/nikita/projects/mle-project-sprint-1-v001/part2_dvc/.env")

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")


def send_telegram_success_message(context):

    hook = TelegramHook(
        token=TELEGRAM_TOKEN,
        chat_id=TELEGRAM_CHAT_ID
    )

    dag = context['dag'].dag_id
    run_id = context['run_id']

    message = f'Исполнение DAG {dag} с id={run_id} прошло успешно!'

    hook.send_message({
        'chat_id': TELEGRAM_CHAT_ID,
        'text': message
    })


def send_telegram_failure_message(context):

    hook = TelegramHook(
        token=TELEGRAM_TOKEN,
        chat_id=TELEGRAM_CHAT_ID
    )

    dag = context['dag'].dag_id
    run_id = context['run_id']
    task_id = context['task_instance'].task_id
    exception = context.get('exception')

    message = (
        f'❌ Исполнение DAG {dag} с id={run_id} завершилось ошибкой.\n'
        f'Task: {task_id}\n'
        f'Ошибка: {exception}'
    )

    hook.send_message({
        'chat_id': TELEGRAM_CHAT_ID,
        'text': message
    })