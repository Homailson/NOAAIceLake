#!/bin/bash
set -e  # Encerra o script se qualquer comando falhar

# Variáveis
REPO_NAME="noaa-ice-lake-lambda"  # Nome do repositório no ECR
LOCAL_IMAGE_NAME="$REPO_NAME-image"  # Nome da imagem local construída
TAG="latest"
ACCOUNT_ID="552683392050"
REGION="us-east-1"
ECR_URI="$ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/$REPO_NAME"

echo "Verificando se o repositório ECR existe..."
# Verificar se o repositório ECR existe, se não, criar
if ! aws ecr describe-repositories --repository-names $REPO_NAME --region $REGION &> /dev/null; then
    echo "Criando repositório ECR: $REPO_NAME"
    aws ecr create-repository --repository-name $REPO_NAME --region $REGION
else
    echo "Repositório ECR já existe: $REPO_NAME"
    
    # Limpar imagens antigas do repositório
    echo "Removendo imagens antigas do ECR..."
    
    # Remover imagens sem tag
    IMAGE_IDS=$(aws ecr list-images --repository-name $REPO_NAME --region $REGION --query 'imageIds[?type(imageTag)!=`string`].[imageDigest]' --output text)
    if [ ! -z "$IMAGE_IDS" ]; then
        echo "Removendo imagens sem tag..."
        aws ecr batch-delete-image --repository-name $REPO_NAME --region $REGION --image-ids $(echo "$IMAGE_IDS" | sed 's/^/imageDigest=/' | tr '\n' ' ')
    fi
    
    # Remover todas as tags exceto 'latest'
    OLD_TAGS=$(aws ecr list-images --repository-name $REPO_NAME --region $REGION --query 'imageIds[?imageTag!=`latest`].imageTag' --output text)
    if [ ! -z "$OLD_TAGS" ]; then
        echo "Removendo imagens com tags antigas..."
        for tag in $OLD_TAGS; do
            echo "Removendo imagem com tag: $tag"
            aws ecr batch-delete-image --repository-name $REPO_NAME --region $REGION --image-ids imageTag=$tag
        done
    fi
fi

# Build da imagem local
echo "Construindo imagem Docker..."
docker build -t $LOCAL_IMAGE_NAME:$TAG .

# Login no ECR
echo "Fazendo login no ECR..."
aws ecr get-login-password --region $REGION | docker login --username AWS --password-stdin $ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com

# Tag da imagem para o ECR
echo "Tageando imagem para o ECR..."
docker tag $LOCAL_IMAGE_NAME:$TAG $ECR_URI:$TAG

# Push da imagem para o ECR
echo "Enviando imagem para o ECR..."
docker push $ECR_URI:$TAG

# Lista de funções Lambda que usam a mesma imagem
LAMBDA_FUNCTIONS=("generatePeriods" "getStationsIds" "getStationsByIds" "getStationsResults" "resultsTrasnformation" "stationsTransformation")

# Contador para funções atualizadas
UPDATED_COUNT=0
SKIPPED_COUNT=0

echo "Atualizando funções Lambda com a nova imagem..."
# Atualizar cada função Lambda na lista
for func in "${LAMBDA_FUNCTIONS[@]}"; do
  echo "Verificando função Lambda: $func"
  if aws lambda get-function --function-name $func --region $REGION &> /dev/null; then
    echo "Atualizando função Lambda existente: $func"
    # Atualizar código da função Lambda
    aws lambda update-function-code \
      --function-name $func \
      --image-uri $ECR_URI:$TAG \
      --publish \
      --region $REGION
    echo "✅ Função $func atualizada com sucesso"
    UPDATED_COUNT=$((UPDATED_COUNT+1))
  else
    echo "⚠️ Função Lambda não encontrada: $func (pulando)"
    SKIPPED_COUNT=$((SKIPPED_COUNT+1))
  fi
done

echo "Implantação concluída com sucesso!"
echo "Funções atualizadas: $UPDATED_COUNT"
echo "Funções puladas: $SKIPPED_COUNT"