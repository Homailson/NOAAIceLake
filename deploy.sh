#!/bin/bash
set -e  # Encerra o script se qualquer comando falhar

# Variáveis
REPO_NAME="noaa-ice-lake-lambda"      # Nome do repositório no ECR
LOCAL_IMAGE_NAME="$REPO_NAME-image"  # Nome da imagem local construída
TAG="latest"
ACCOUNT_ID="552683392050"
REGION="us-east-1"
ECR_URI="$ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/$REPO_NAME"

echo "Verificando se o repositório ECR existe..."
if ! aws ecr describe-repositories --repository-names "$REPO_NAME" --region "$REGION" &> /dev/null; then
  echo "Criando repositório ECR: $REPO_NAME"
  aws ecr create-repository --repository-name "$REPO_NAME" --region "$REGION"
else
  echo "Repositório ECR já existe: $REPO_NAME"
fi

# Build da imagem local (FORÇANDO single-arch e single-manifest compatível com Lambda)
echo "Construindo imagem Docker (linux/amd64, single manifest)..."
docker buildx build \
  --platform linux/amd64 \
  --output=type=docker \
  -t "$LOCAL_IMAGE_NAME:$TAG" .

# Login no ECR
echo "Fazendo login no ECR..."
aws ecr get-login-password --region "$REGION" | docker login --username AWS --password-stdin "$ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com"

# Tag da imagem para o ECR
echo "Tageando imagem para o ECR..."
docker tag "$LOCAL_IMAGE_NAME:$TAG" "$ECR_URI:$TAG"

# Push da imagem para o ECR
echo "Enviando imagem para o ECR..."
docker push "$ECR_URI:$TAG"

# Validar mediaType no ECR (Lambda NÃO aceita OCI index)
echo "Validando o imageManifestMediaType no ECR..."
MEDIA_TYPE=$(aws ecr batch-get-image \
  --repository-name "$REPO_NAME" \
  --image-ids imageTag="$TAG" \
  --region "$REGION" \
  --query 'images[0].imageManifestMediaType' \
  --output text)

echo "ECR mediaType: $MEDIA_TYPE"

if [[ "$MEDIA_TYPE" == "application/vnd.oci.image.index.v1+json" ]]; then
  echo "❌ ERRO: A imagem foi publicada como OCI index (multi-arch / manifest list). A AWS Lambda não suporta isso."
  echo "✅ Solução: mantenha o build com --output=type=docker (já está), e garanta que você NÃO está usando buildx --push direto."
  exit 1
fi

echo "✅ Imagem OK para Lambda (single manifest)."

# Lista de funções Lambda que usam a mesma imagem
LAMBDA_FUNCTIONS=("generatePeriods" "getStationsIds" "getStationsByIds" "getStationsResults" "resultsTrasnformation" "stationsTransformation" "presentation")

# Contadores
UPDATED_COUNT=0
SKIPPED_COUNT=0

echo "Atualizando funções Lambda com a nova imagem..."
for func in "${LAMBDA_FUNCTIONS[@]}"; do
  echo "Verificando função Lambda: $func"
  if aws lambda get-function --function-name "$func" --region "$REGION" &> /dev/null; then
    echo "Atualizando função Lambda existente: $func"
    aws lambda update-function-code \
      --function-name "$func" \
      --image-uri "$ECR_URI:$TAG" \
      --publish \
      --region "$REGION"
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