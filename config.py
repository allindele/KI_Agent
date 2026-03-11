import os
import logging
from dotenv import load_dotenv

load_dotenv()
SENDER_EMAIL = os.getenv("SENDER_EMAIL")
SENDER_PASSWORD = os.getenv("SENDER_PASSWORD")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
PUBLIC_URL = os.getenv("PUBLIC_URL")

PENDING_BATCHES = {}

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger("TechCorp_AI")
