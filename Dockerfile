# Imagem da lambda com python da AWS
FROM public.ecr.aws/lambda/python:3.11

# Configurando HOME para /tmp buscando evitar IO error
ENV HOME=/tmp

# Diretório de trabalho do container
WORKDIR /var/task

# Copia somete o necessário
COPY requirements.txt ./

# Rodando pip para instalar bibliotecas
RUN pip install -r requirements.txt

# Copiando tudo para o diretório de trabalho
COPY . ./