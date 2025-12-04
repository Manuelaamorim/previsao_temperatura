# Pipeline de BI para Prever Temperatura Horária

## 1. Visão Geral

Este projeto implementa um pipeline completo de Business Intelligence (BI) para ingestão, processamento, modelagem e visualização de dados meteorológicos do INMET, desenvolvido para a disciplina Análise e Visualização de Dados — CESAR School (2025.2).

O objetivo do grupo foi prever a temperatura com 1 hora de antecedência usando um modelo de machine learning treinado sobre variáveis meteorológicas reais.

O pipeline foi construído em uma arquitetura totalmente orquestrada em Docker, integrando:
- FastAPI — ingestão de dados
- PostgreSQL — armazenamento estruturado
- MinIO (S3) — armazenamento de artefatos do modelo
- MLflow — rastreamento e versionamento do modelo
- Jupyter Notebook — análise e treinamento
- ThingsBoard — dashboard final em tempo real

Variáveis utilizadas:
- Temperatura instantânea
- Umidade relativa do ar
- Velocidade do vento
- Direção do vento
- Radiação solar
- Precipitação

A visualização final inclui série temporal de temperatura real vs. prevista e indicadores de erro, como RMSE.

## 2. Arquitetura do Pipeline

A solução segue o fluxo:
- O simulador envia dados hora a hora para a FastAPI
- A FastAPI grava os registros no PostgreSQL
- O notebook lê, trata e treina o modelo
- O modelo é registrado no MLflow e armazenado no MinIO
- A FastAPI recarrega o modelo treinado
- Um script envia previsões contínuas para o ThingsBoard, atualizando o dashboard

Serviços orquestrados pelo docker-compose:
Serviço	     Porta	        Finalidade
PostgreSQL	  5432	      Banco de dados
MinIO	      9000/9001	    Armazenamento S3
MLflow	      5050	   Versionamento de modelo
Jupyter	      8888	    Análise e treinamento
FastAPI	      8000	    Ingestão e predição
ThingsBoard	  9090	   Dashboard em tempo real

## 3. Como Executar o Pipeline

### ETAPA 1 — Clonar o Repositório
git clone https://github.com/Manuelaamorim/previsao_temperatura.git
cd previsao_temperatura

### ETAPA 2 — Subir a Infraestrutura Docker
docker-compose up -d --build

Aguarde 2 a 3 minutos

Verifique:
docker ps
Você deve encontrar 6 containers rodando

### ETAPA 3 — Popular o Banco de Dados
Rodar o simulador por ~5 minutos:
python simulador.py

Pare com Ctrl+C após ~200 registros.

Verifique na API:
Acesse:
http://localhost:8000/docs

Teste o endpoint:
GET /data/history

### ETAPA 4 — Treinar e Otimizar o Modelo

Acesse o Jupyter:
http://localhost:8888
Token: cesar123

Dentro do Jupyter:
Abra o notebook 03_otimizacao_modelo.ipynb
Execute todas as células (Shift+Enter)
Aguarde ~5–10 minutos

Verifique no MLflow:
http://localhost:5050
Procure o experimento: Weather_Temperature_Prediction
Métricas esperadas: RMSE, MAE, R²

### ETAPA 5 — Recarregar Modelo na FastAPI

Após o treinamento, reinicie o container:
docker-compose restart fastapi

Teste a API:
GET /model/status - deve exibir "model_loaded": true
GET /predict/auto - deve retornar uma previsão

### ETAPA 6 — Integrar com o ThingsBoard

Rodar o script que envia previsões contínuas:
python enviar_previsao_thingsboard.py

Acesse o dashboard:
http://localhost:9090
Login: tenant@thingsboard.org
Senha: tenant

## 4. Resumo das URLs
Serviço	      URL	                         Credenciais
Jupyter	      http://localhost:8888        Token: cesar123
MLflow	      http://localhost:5050        –
FastAPI Docs	http://localhost:8000/docs   –
MinIO Console	http://localhost:9001        minioadmin / minioadmin
ThingsBoard	  http://localhost:9090        tenant@thingsboard.org / tenant

## Equipe / Github:

1. Beatriz Carla Pereira - biapereira2
2. Hugo Alcantara da Rocha - 
3. Manuela Cavalcanti - Manuelaamorim
4. Maria Fernanda Ordonho - nandaord
5. Rafaela Brasileiro Vidal - Rafabvidal
6. Ygor Rosa - YgoRosa
