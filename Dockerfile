# Imagem da lambda com python da AWS
FROM public.ecr.aws/lambda/python:3.11

# Configurando HOME para /tmp buscando evitar IO error
ENV HOME=/tmp

# Configurando variáveis de ambiente para o PyIceberg
ENV PYICEBERG_CATALOG__GLUE__TYPE=glue
ENV PYICEBERG_CATALOG__GLUE__URI=https://glue.us-east-1.amazonaws.com
ENV PYICEBERG_CATALOG__GLUE__WAREHOUSE=s3://noaaicelake


# Diretório de trabalho do container
WORKDIR /var/task

# Copia somete o necessário
COPY requirements-no-hash.txt ./

# Rodando pip para instalar bibliotecas
RUN pip install --no-cache-dir -r requirements-no-hash.txt

# Copiando tudo para o diretório de trabalho
COPY . ./