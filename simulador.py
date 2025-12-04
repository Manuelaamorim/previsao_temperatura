import pandas as pd
import requests
import time

API_URL = "http://localhost:8000/ingest"
TB_HOST = "http://localhost:9090"
TB_TOKEN = 'zb4uP0BTV7zGsvPnu9IG'
TB_URL = f"{TB_HOST}/api/v1/{TB_TOKEN}/telemetry"
CSV_FILE = "notebooks/data/dados_tratados.csv"
DELAY_SEGUNDOS = 0.5


def enviar_para_api(payload):
    """Envia para o Banco de Dados (Via FastAPI)"""
    try:
        response = requests.post(API_URL, json=payload)
        return response.status_code == 200
    except:
        return False


def enviar_para_thingsboard(payload):
    """Envia para o ThingsBoard (Visualização)"""
    try:
        response = requests.post(TB_URL, json=payload)
        return response.status_code == 200
    except:
        return False


print("=" * 60)
print("   INGESTÃO DE DADOS TRATADOS - ESTAÇÃO INMET")
print("=" * 60)
print(f"📂 Fonte: {CSV_FILE}")
print(f"🔗 API: {API_URL}")
print(f"📊 ThingsBoard: {TB_URL}")
print("=" * 60)

try:
    df = pd.read_csv(CSV_FILE, parse_dates=['datetime'])
    
    print(f"\n✅ Arquivo carregado com sucesso!")
    print(f"📊 Total de registros: {len(df)} linhas")
    print(f"📅 Período: {df['datetime'].iloc[0]} até {df['datetime'].iloc[-1]}")
    print(f"\n📋 Colunas disponíveis: {list(df.columns)}")
    print("\n🚀 Iniciando envio de dados tratados...\n")

    for index, row in df.iterrows():
        dt = row['datetime']
        payload = {
            "station_code": "A301",  # Código da estação INMET
            "temperature": float(row['Temp']),
            "humidity": float(row['Umi']),
            "pressure": 1013.25,  # Valor padrão
            "wind_speed": float(row['Vento']),
            "wind_direction": 0.0,  # Valor padrão
            "radiation": float(row['Rad']),
            "precipitation": float(row['Chuva'])
        }
        
        api_ok = enviar_para_api(payload)
        tb_ok = enviar_para_thingsboard(payload)
        
        # Status visual
        status_api = "✅" if api_ok else "❌"
        status_tb = "✅" if tb_ok else "❌"
        
        # Formata datetime para exibição
        dt_str = dt.strftime("%Y-%m-%d %H:%M") if hasattr(dt, 'strftime') else str(dt)[:16]
        
        print(f"[{index+1}/{len(df)}] {dt_str} | "
              f"Temp: {payload['temperature']:.1f}°C | "
              f"Umi: {payload['humidity']:.0f}% | "
              f"Vento: {payload['wind_speed']:.1f}m/s | "
              f"API:{status_api} TB:{status_tb}")
        
        time.sleep(DELAY_SEGUNDOS)
    
    print("\n" + "=" * 60)
    print("✅ INGESTÃO CONCLUÍDA!")
    print(f"   Total de registros enviados: {len(df)}")
    print("=" * 60)

except FileNotFoundError:
    print(f"❌ Erro: O arquivo '{CSV_FILE}' não foi encontrado.")
except Exception as e:
    print(f"❌ Erro inesperado: {e}")