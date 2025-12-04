import os
import json
import boto3
import logging
import pickle
import numpy as np
import pandas as pd
from datetime import datetime
from typing import List, Optional
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, Float, String, DateTime, desc
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker
from botocore.exceptions import NoCredentialsError

# Configuração de Logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Weather Prediction API",
    description="API de Ingestão e Previsão de Temperatura",
    version="2.0"
)

# CORS para permitir chamadas do ThingsBoard/Trendz
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- CONFIGURAÇÕES DE AMBIENTE ---
DB_URL = os.getenv("DATABASE_URL", "postgresql://admin:secret@db:5432/weather_db")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY", "minioadmin")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY", "minioadmin")
BUCKET_NAME = "weather-raw-data"

# --- CONFIGURAÇÃO DO BANCO (POSTGRES) ---
Base = declarative_base()

class WeatherData(Base):
    __tablename__ = "weather_measurements"
    
    id = Column(Integer, primary_key=True, index=True)
    timestamp = Column(DateTime, default=datetime.utcnow)
    station_code = Column(String)
    temperature = Column(Float)  # Temperatura do ar
    humidity = Column(Float)     # Umidade relativa
    pressure = Column(Float)     # Pressão atmosférica
    wind_speed = Column(Float)   # Velocidade do vento
    wind_direction = Column(Float) # Direção do vento
    radiation = Column(Float)    # Radiação solar
    precipitation = Column(Float)# Precipitação

# Cria a conexão e a tabela
engine = create_engine(DB_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
try:
    Base.metadata.create_all(bind=engine)
    logger.info("Tabela PostgreSQL verificada/criada com sucesso.")
except Exception as e:
    logger.error(f"Erro ao conectar no Banco: {e}")

# --- CONFIGURAÇÃO DO MINIO (S3) ---
s3_client = boto3.client(
    's3',
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY
)

def ensure_bucket_exists():
    try:
        s3_client.create_bucket(Bucket=BUCKET_NAME)
        logger.info(f"Bucket '{BUCKET_NAME}' criado/verificado.")
    except s3_client.exceptions.BucketAlreadyOwnedByYou:
        pass
    except Exception as e:
        logger.error(f"Erro ao criar bucket MinIO: {e}")

# --- MODELO DE DADOS (VALIDAÇÃO) ---
class WeatherInput(BaseModel):
    station_code: str
    temperature: float
    humidity: float
    pressure: float
    wind_speed: float
    wind_direction: float
    radiation: float
    precipitation: float

# --- ENDPOINT DE INGESTÃO ---
@app.post("/ingest")
def ingest_data(data: WeatherInput):
    # 1. Salvar no MinIO (JSON Bruto)
    try:
        ensure_bucket_exists()
        file_name = f"{data.station_code}_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"
        json_data = json.dumps(data.dict())
        s3_client.put_object(Bucket=BUCKET_NAME, Key=file_name, Body=json_data)
        logger.info(f"Dados salvos no MinIO: {file_name}")
    except Exception as e:
        logger.error(f"Falha ao salvar no S3: {e}")
        # Não paramos o processo, tentamos salvar no banco mesmo assim

    # 2. Salvar no PostgreSQL (Estruturado)
    db = SessionLocal()
    try:
        db_item = WeatherData(**data.dict())
        db.add(db_item)
        db.commit()
        db.refresh(db_item)
        logger.info(f"Dados salvos no Postgres ID: {db_item.id}")
    except Exception as e:
        logger.error(f"Falha ao salvar no DB: {e}")
        raise HTTPException(status_code=500, detail="Erro ao salvar no banco de dados")
    finally:
        db.close()

    return {"status": "received", "s3_saved": True, "db_saved": True}


# =============================================================================
# MODELO DE PREVISÃO - CARREGAMENTO E ENDPOINT
# =============================================================================

# Variável global para o modelo
loaded_model = None
model_features = None
model_lags = None
model_metrics = None

def load_prediction_model():
    """Carrega o modelo otimizado do arquivo pickle."""
    global loaded_model, model_features, model_lags, model_metrics
    
    model_path = "/app/models/rf_optimized_model.pkl"
    
    # Fallback para desenvolvimento local
    if not os.path.exists(model_path):
        model_path = "models/rf_optimized_model.pkl"
    
    if not os.path.exists(model_path):
        logger.warning(f"Modelo não encontrado em {model_path}")
        return False
    
    try:
        with open(model_path, 'rb') as f:
            model_data = pickle.load(f)
        
        loaded_model = model_data['model']
        model_features = model_data['features']
        model_lags = model_data['lags']
        model_metrics = model_data.get('metrics', {})
        
        logger.info(f"✅ Modelo carregado com sucesso! Features: {len(model_features)}")
        return True
    except Exception as e:
        logger.error(f"Erro ao carregar modelo: {e}")
        return False


# Modelos de entrada/saída para o endpoint de previsão
class PredictionInput(BaseModel):
    """Dados de lag para previsão (últimas 24 horas de dados)."""
    Temp_lag_1: float
    Temp_lag_2: float
    Temp_lag_3: float
    Temp_lag_6: float
    Temp_lag_12: float
    Temp_lag_24: float
    Umi_lag_1: float
    Umi_lag_2: float
    Umi_lag_3: float
    Umi_lag_6: float
    Umi_lag_12: float
    Umi_lag_24: float
    Vento_lag_1: float
    Vento_lag_2: float
    Vento_lag_3: float
    Vento_lag_6: float
    Vento_lag_12: float
    Vento_lag_24: float
    Rad_lag_1: float
    Rad_lag_2: float
    Rad_lag_3: float
    Rad_lag_6: float
    Rad_lag_12: float
    Rad_lag_24: float
    hour_sin: float
    hour_cos: float


class PredictionOutput(BaseModel):
    """Resultado da previsão."""
    predicted_temperature: float
    prediction_time: str
    model_rmse: Optional[float] = None
    model_mae: Optional[float] = None
    confidence_interval: Optional[dict] = None


class AutoPredictionOutput(BaseModel):
    """Resultado da previsão automática (usa dados do DB)."""
    predicted_temperature: float
    current_temperature: float
    prediction_time: str
    data_timestamp: str
    model_metrics: Optional[dict] = None


# Endpoint de previsão manual
@app.post("/predict", response_model=PredictionOutput, tags=["Prediction"])
def predict_temperature(data: PredictionInput):
    """
    Realiza previsão de temperatura para T+1h.
    
    Recebe os dados de lag (últimas 24 horas) e retorna a previsão.
    """
    global loaded_model, model_features, model_metrics
    
    # Carregar modelo se ainda não carregado
    if loaded_model is None:
        if not load_prediction_model():
            raise HTTPException(
                status_code=503, 
                detail="Modelo de previsão não disponível. Execute o notebook de otimização primeiro."
            )
    
    try:
        # Criar DataFrame com as features na ordem correta
        input_data = pd.DataFrame([data.dict()])
        
        # Reordenar colunas conforme o modelo espera
        if model_features:
            input_data = input_data[model_features]
        
        # Fazer previsão
        prediction = loaded_model.predict(input_data)[0]
        
        # Calcular intervalo de confiança aproximado (±RMSE)
        rmse = model_metrics.get('rmse', 0)
        confidence = {
            "lower": prediction - rmse,
            "upper": prediction + rmse
        } if rmse > 0 else None
        
        return PredictionOutput(
            predicted_temperature=round(prediction, 2),
            prediction_time=datetime.now().isoformat(),
            model_rmse=model_metrics.get('rmse'),
            model_mae=model_metrics.get('mae'),
            confidence_interval=confidence
        )
        
    except Exception as e:
        logger.error(f"Erro na previsão: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao processar previsão: {str(e)}")


# Endpoint de previsão automática (usa dados do banco)
@app.get("/predict/auto", response_model=AutoPredictionOutput, tags=["Prediction"])
def auto_predict_temperature():
    """
    Realiza previsão automática usando os últimos dados do banco.
    
    Busca as últimas 24 horas de dados do PostgreSQL e calcula a previsão.
    """
    global loaded_model, model_features, model_lags, model_metrics
    
    # Carregar modelo se ainda não carregado
    if loaded_model is None:
        if not load_prediction_model():
            raise HTTPException(
                status_code=503, 
                detail="Modelo de previsão não disponível."
            )
    
    db = SessionLocal()
    try:
        # Buscar os últimos 25 registros (para ter dados suficientes para lags)
        recent_data = db.query(WeatherData).order_by(desc(WeatherData.timestamp)).limit(25).all()
        
        if len(recent_data) < 25:
            raise HTTPException(
                status_code=400, 
                detail=f"Dados insuficientes. Necessário 25 registros, encontrado: {len(recent_data)}"
            )
        
        # Converter para DataFrame (reverter ordem para cronológica)
        df = pd.DataFrame([{
            'timestamp': r.timestamp,
            'Temp': r.temperature,
            'Umi': r.humidity,
            'Vento': r.wind_speed,
            'Rad': r.radiation
        } for r in reversed(recent_data)])
        
        df = df.set_index('timestamp')
        
        # Calcular features de lag
        features = {}
        lags = model_lags or [1, 2, 3, 6, 12, 24]
        
        for lag in lags:
            idx = -lag - 1  # índice relativo ao último registro
            if abs(idx) <= len(df):
                features[f'Temp_lag_{lag}'] = df['Temp'].iloc[idx]
                features[f'Umi_lag_{lag}'] = df['Umi'].iloc[idx]
                features[f'Vento_lag_{lag}'] = df['Vento'].iloc[idx]
                features[f'Rad_lag_{lag}'] = df['Rad'].iloc[idx]
        
        # Features cíclicas baseadas na hora atual
        current_hour = datetime.now().hour
        features['hour_sin'] = np.sin(2 * np.pi * current_hour / 24)
        features['hour_cos'] = np.cos(2 * np.pi * current_hour / 24)
        
        # Criar DataFrame para previsão
        input_data = pd.DataFrame([features])
        
        # Reordenar colunas
        if model_features:
            input_data = input_data[model_features]
        
        # Fazer previsão
        prediction = loaded_model.predict(input_data)[0]
        
        return AutoPredictionOutput(
            predicted_temperature=round(prediction, 2),
            current_temperature=round(df['Temp'].iloc[-1], 2),
            prediction_time=datetime.now().isoformat(),
            data_timestamp=str(df.index[-1]),
            model_metrics=model_metrics
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro na previsão automática: {e}")
        raise HTTPException(status_code=500, detail=f"Erro: {str(e)}")
    finally:
        db.close()


# Endpoint de status do modelo
@app.get("/model/status", tags=["Model"])
def model_status():
    """Retorna informações sobre o modelo carregado."""
    global loaded_model, model_features, model_metrics
    
    if loaded_model is None:
        load_prediction_model()
    
    return {
        "model_loaded": loaded_model is not None,
        "features_count": len(model_features) if model_features else 0,
        "features": model_features,
        "metrics": model_metrics,
        "model_type": type(loaded_model).__name__ if loaded_model else None
    }


# Endpoint de histórico para visualização
@app.get("/data/history", tags=["Data"])
def get_history(limit: int = 100):
    """
    Retorna os últimos N registros para visualização.
    """
    db = SessionLocal()
    try:
        records = db.query(WeatherData).order_by(desc(WeatherData.timestamp)).limit(limit).all()
        
        return [{
            "timestamp": r.timestamp.isoformat(),
            "temperature": r.temperature,
            "humidity": r.humidity,
            "wind_speed": r.wind_speed,
            "radiation": r.radiation,
            "precipitation": r.precipitation
        } for r in reversed(records)]
    finally:
        db.close()


# Carregar modelo na inicialização
@app.on_event("startup")
async def startup_event():
    """Carrega o modelo na inicialização da API."""
    logger.info("Iniciando Weather Prediction API...")
    load_prediction_model()