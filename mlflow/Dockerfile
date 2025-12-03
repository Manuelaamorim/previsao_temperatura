FROM python:3.9-slim

WORKDIR /app

# Instala dependências do sistema (necessário para o driver do Postgres)
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Instala o MLflow e os conectores necessários
# boto3: para conectar no MinIO (S3)
# psycopg2-binary: para conectar no PostgreSQL
RUN pip install --no-cache-dir \
    mlflow==2.7.1 \
    boto3 \
    psycopg2-binary

# Expõe a porta padrão do MLflow
EXPOSE 5000

# O comando de inicialização (CMD) já está definido no docker-compose.yml,
# então não precisamos repetir aqui.