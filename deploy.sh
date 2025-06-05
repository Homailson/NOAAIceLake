#!/bin/bash

# Variáveis
REPO_NAME="noaa-ice-lake-lambda"
IMAGE_NAME="$REPO_NAME:latest"
ACCOUNT_ID="552683392050"
REGION="us-east-1"
ECR_URI="$ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/$REPO_NAME"
FUNCTION_NAME="noaa-ice-lake-image"

# Build da imagem
docker build -t $IMAGE_NAME .

# Login no ECR
aws ecr get-login-password --region $REGION | docker login --username AWS --password-stdin $ECR_URI

# Tag da imagem
docker tag $IMAGE_NAME $ECR_URI:latest

# Push da imagem
docker push $ECR_URI:latest

# Atualização da função Lambda
aws lambda update-function-code \
  --function-name $FUNCTION_NAME \
  --image-uri $ECR_URI:latest \
  --publish