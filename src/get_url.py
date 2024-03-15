import argparse
import logging
import os
import time
from functools import wraps

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def log_function_call(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        logging.info(f"{timestamp} - Calling {func.__name__} with args: {args}, kwargs: {kwargs}")
        return func(*args, **kwargs)
    return wrapper

@log_function_call
def get_data(cap: str, year: str, month: str):
    year = str(year)
    month = str(month).zfill(2)

    if cap == "yellow":
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month}.parquet"
    elif cap == "green":
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{month}.parquet"
    else:
        raise ValueError("Invalid cab type. Choose either 'yellow' or 'green'")
    return url

def main(args):

    # Create the output file if it doesn't exist
    if not os.path.exists(args.output_file):
        with open(args.output_file, "w"):
            pass


    url = get_data(args.type, args.year, args.month)

    with open(args.output_file, "w") as file:
            file.write(url)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate URLs for NYC taxi trip data")
    parser.add_argument("--type", "-t", choices=["yellow", "green"], required=True, help="Specify the type of taxi data (yellow or green)")
    parser.add_argument("--year", "-y", type=int, required=True, help="Specify the year")
    parser.add_argument("--month", "-m", type=int, required=True, help="Specify the month")
    parser.add_argument("--output-file", "-o", default="download_urls.txt", help="Specify the output file to write URLs")
    args = parser.parse_args()

    main(args)
