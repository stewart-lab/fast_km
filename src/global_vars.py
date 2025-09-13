import os

# data directory to store databases
data_dir = "_data"
if not os.path.exists(data_dir):
    full_data_dir_path = os.path.abspath(data_dir)
    print(f"{full_data_dir_path} directory does not exist, creating it...")
    os.makedirs(full_data_dir_path, exist_ok=True)

# connection info
redis_host = "no_redis_host"
redis_port = 0
fastapi_port = 0

# censoring year limits for KM/SKiM searches
MIN_CENSOR_YEAR = 1000
MAX_CENSOR_YEAR = 2100

# API keys
SECRET_PUBMED_API_KEY = ""
SECRET_OPENAI_API_KEY = ""
SECRET_HTCONDOR_TOKEN = ""
SECRET_DEEPSEEK_API_KEY = ""

# timezone for dashboard
timezone = "America/Chicago"