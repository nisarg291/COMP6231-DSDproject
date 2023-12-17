from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Access the GOOGLE_APPLICATION_CREDENTIALS variable
credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')