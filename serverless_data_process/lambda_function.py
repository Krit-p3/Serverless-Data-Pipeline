import boto3 
from lib import *  
from io import BytesIO


s3_client = boto3.client('s3')

def handler(event, context):
    
    # Get the s3 bucket name and key 
    
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    object_key = event['Records'][0]['s3']['object']['key']



    # Get object
    response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
    buffer_data = response['Body'].read()
    buffer_data = BytesIO(buffer_data)
    #test/yellow_tripdata_2020-07.parquet
    
    values = object_key.split('/')[-1].split('_')[0]

    #pattern = r'(.*?)_tripdata_\d{4}-\d{2}.parquet'
    #match = re.search(pattern, object_key)

    #values = match.group(1)

    data = read_data(buffer_data)

    df = data_wrangling(data, values)

    df = clean_data(df)


    fact_trip_path = "s3://deltalake-obj/nyc_taxi_data/delta_table/fact_trip/"
    fact_trip_table(fact_trip_path, df)

    dim_table_path = "s3://deltalake-obj/nyc_taxi_data/delta_table/dim_time/"
    dim_time_table(dim_table_path,df)

    return "Success!!"

