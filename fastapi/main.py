import os
import json
import boto3
import logging
from datetime import datetime
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, Float, String, DateTime
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker
from botocore.exceptions import NoCredentialsError

# Configuração de Logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Weather Ingestion API")

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