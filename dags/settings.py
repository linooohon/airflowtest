import os
from dotenv import load_dotenv
load_dotenv()

SLACK_WEBHOOK_TOKEN = os.getenv("SLACK_WEBHOOK_TOKEN")