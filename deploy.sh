#!/bin/bash

# Variáveis
REPO_NAME="noaa-ice-lake-lambda"  # Nome do repositório no ECR
LOCAL_IMAGE_NAME="noaa-ice-lake-lambda-image"  # Nome da imagem local construída
TAG="latest"
ACCOUNT_ID="552683392050"
REGION="us-east-1"
ECR_URI="$ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/$REPO_NAME"
FUNCTION_NAME="noaa-ice-lake-ingestion"  # Nome da função Lambda

# Build da imagem local (remova se já estiver construída)
docker build -t $LOCAL_IMAGE_NAME:$TAG .

# Login no ECR
aws ecr get-login-password --region $REGION | docker login --username AWS --password-stdin $ECR_URI

# Tag da imagem para o ECR
docker tag $LOCAL_IMAGE_NAME:$TAG $ECR_URI:$TAG

# Push da imagem para o ECR
docker push $ECR_URI:$TAG

# Atualizar código da função Lambda
aws lambda update-function-code \
  --function-name $FUNCTION_NAME \
  --image-uri $ECR_URI:$TAG \
  --publish