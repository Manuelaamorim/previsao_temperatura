FROM python:3.9-slim

WORKDIR /app

# Instala dependências do sistema necessárias para o Postgres e compilação
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copia e instala as bibliotecas Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Criar diretório para modelos
RUN mkdir -p /app/models

# Copia o código da aplicação
COPY . .

# Comando para rodar a API na porta 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]