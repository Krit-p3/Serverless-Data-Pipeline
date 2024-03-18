#!/bin/bash

# Define variables
ACCOUNT_ID="<your_account_id>"
ROLE_NAME="<your_role_name>"
ECR_REPOSITORY_URI="$ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com"
IMAGE_NAME="data-processing:test"
FUNCTION_NAME="data-processing"
ROLE_ARN="arn:aws:iam::$ACCOUNT_ID:role/$ROLE_NAME" 

# Tag the Docker image
docker tag $IMAGE_NAME $ECR_REPOSITORY_URI/data_processing:latest

# Push the Docker image to Amazon ECR
docker push $ECR_REPOSITORY_URI/data_processing:latest

# Create AWS Lambda function
aws lambda create-function \
  --function-name $FUNCTION_NAME \
  --package-type Image \
  --code ImageUri=$ECR_REPOSITORY_URI/data_processing:latest \
  --role $ROLE_ARN

