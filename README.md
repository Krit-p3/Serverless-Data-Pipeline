# Serverless Data Pipeline with AWS Lambda

This guide provides step-by-step instructions to set up a serverless data pipeline using AWS Lambda for processing raw data into a Delta table without the need for Databricks or Spark. This approach is suitable for handling data sizes up to 400 MB.

By leveraging AWS Lambda, a serverless compute service, we can design and deploy a lightweight data pipeline that efficiently processes raw data and stores it in a Delta table on Amazon S3. This approach offers scalability, cost-effectiveness, and simplicity, making it ideal for scenarios where the data size is relatively small and the infrastructure requirements are minimal.

## Prerequisites
Before starting, ensure you have:
- An AWS account
- AWS CLI installed and configured

## Step 1: Create Bucket and Objects
1. Create an S3 bucket named `nyc_taxi_data`.
2. Inside the bucket, create two objects: `raw_data` and `delta_table`.

## Step 2: Create AWS Lambda for Data Processing
1. Deploy an AWS Lambda function.
2. Configure the Lambda function to trigger when data is put into the `raw_data` object and store the processed data in the `delta_table` object.
3. Set the Lambda function environment variable configuration with `AWS_S3_ALLOW_UNSAFE_RENAME = true`.
4. Assign an IAM role with appropriate permissions to the Lambda function and S3 bucket.

## Step 3: Run `download_parquet.sh` Script
1. Run the `download_parquet.sh` script to download the required files and upload them to the `raw_data` object in the S3 bucket.

## Step 4: Example Analysis from S3 Directly
1. Utilize the `nyc_taxi_status.py` script to perform analysis directly on the data stored in the S3 bucket.

## Step 5: Examine Delta Table
1. Use the `examine.py` script to examine the Delta table, including metadata, schema, history, and current add actions.

## Conclusion
By following these steps, you can set up a serverless data pipeline on AWS Lambda for processing and analyzing raw data efficiently. This approach allows for scalable and cost-effective data processing, making it suitable for various data processing tasks without relying on Databricks or Spark.
