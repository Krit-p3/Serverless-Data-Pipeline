#import s3fs 
from deltalake import DeltaTable, write_deltalake
import boto3
from botocore.exceptions import NoCredentialsError
import polars as pl

def get_aws_credentials():
    try:
        session = boto3.Session()

        credentials = session.get_credentials()

        access_key = credentials.access_key

        secret_key = credentials.secret_key

        return access_key, secret_key

    except NoCredentialsError:
        print("AWS credentials not found")
        return None 



def get_delta_table(table_name: str):
    
    
    access_key, secret_key = get_aws_credentials()

    if access_key and secret_key:
        storage_options = {
            "AWS_ACCESS_KEY_ID": access_key, 
            "AWS_SECRET_ACCESS_KEY": secret_key,
            "region": "ap-southeast-1"
        }

        # connect_timeout and read_timeout parameters
        s3_client = boto3.client('s3', 
                                 region_name='ap-southeast-1', 
                                 aws_access_key_id=access_key, 
                                 aws_secret_access_key=secret_key, 
                                 config=boto3.session.Config(connect_timeout=10, read_timeout=60))
        
        dt = DeltaTable(f"s3a://deltalake-obj/nyc_taxi_data/delta_table/{table_name}", storage_options=storage_options)

        #dt = dt.to_pandas()
        #dt = pl.DataFrame(dt)
        return dt 

    else: 
        return None


def write_delta_table(df, table_name: str):
    access_key, secret_key = get_aws_credentials()

    if access_key and secret_key:
        storage_options = {
            "AWS_ACCESS_KEY_ID": access_key, 
            "AWS_SECRET_ACCESS_KEY": secret_key,
            "region": "ap-southeast-1",
            "AWS_S3_ALLOW_UNSAFE_RENAME" : 'true'
        }

        # connect_timeout and read_timeout parameters
        s3_client = boto3.client('s3', 
                                 region_name='ap-southeast-1', 
                                 aws_access_key_id=access_key, 
                                 aws_secret_access_key=secret_key, 
                                 config=boto3.session.Config(connect_timeout=10, read_timeout=60))
        
        path = f"s3a://deltalake-obj/nyc_taxi_data/delta_table/{table_name}"
        dt = write_deltalake(path, df,  storage_options=storage_options)

    else: 
        return None
    



