import os
import logging
from dotenv import load_dotenv

PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
load_dotenv(verbose=True, dotenv_path=os.path.join(PROJECT_ROOT, '.env'))

SERVICE_ID = os.getenv('SERVICE_ID')
SERVICE_ADDRESS = '0.0.0.0'
SERVICE_PORT = os.getenv('PORT', '7357')

LOG_LEVEL = logging.DEBUG