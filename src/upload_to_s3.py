import os
import requests
import logging
import re
import time
import boto3
from botocore.exceptions import ClientError
from functools import wraps

# Configure logging to write in console and a file
logging.basicConfig(level=logging.INFO, filename='upload_to_s3.log', filemode='a')
logger = logging.getLogger(__name__)

# AWS S3
s3_client = boto3.client('s3', 'ap-southeast-1')

def log_function_call(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        logging.info(f"Function {func.__name__} called at {timestamp}")
        result = func(*args, **kwargs)
        logging.info(f"Function {func.__name__} completed at {timestamp}")
        return result
    return wrapper

@log_function_call
def download_pq_and_upload_to_s3(input_file: str, pattern: str, bucket_name: str):
    # Create the temp directory if it doesn't exist
    temp_dir = 'temp'
    os.makedirs(temp_dir, exist_ok=True)
    # Read Urls from input file
    with open(input_file, 'r') as f:
        urls = f.readlines()

    # Iterate each Url and download
    for url in urls:
        url = url.strip()
        try:
            response = requests.get(url, stream=True)
            if response.status_code == 200:
                # Get file name from url
                filename = os.path.basename(url)
                match = re.search(pattern, url)

                if match:
                    year = match.group(1)

                # Save the parquet file locally
                file_path = f"temp/{filename}"
                with open(file_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)

                # Upload file to Aws s3
                s3_key = f"nyc_taxi_data/raws_data/{year}/{filename}"
                s3_client.upload_file(file_path, bucket_name, s3_key)
                logger.info(f"Upload successful: s3://{bucket_name}/{s3_key}")

                os.remove(file_path)

                time.sleep(5)

            else:
                response.raise_for_status()
        except Exception as e:
            logger.exception(f"Error downloading {url}: {str(e)}")

if __name__ == "__main__":
    input_file = "download_urls.txt"
    pattern = r'(\d{4})-(\d{2})\.parquet'
    bucket_name = 'deltalake-obj'

    download_pq_and_upload_to_s3(input_file, pattern, bucket_name)

