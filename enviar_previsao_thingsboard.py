import requests
import time
import json
from datetime import datetime

FASTAPI_URL = "http://localhost:8000"
PREDICT_ENDPOINT = f"{FASTAPI_URL}/predict/auto"
HISTORY_ENDPOINT = f"{FASTAPI_URL}/data/history"
THINGSBOARD_URL = "http://localhost:9090"
DEVICE_ACCESS_TOKEN = "zb4uP0BTV7zGsvPnu9IG"
TELEMETRY_ENDPOINT = f"{THINGSBOARD_URL}/api/v1/{DEVICE_ACCESS_TOKEN}/telemetry"
INTERVALO_SEGUNDOS = 30

def obter_previsao():
    try:
        response = requests.get(PREDICT_ENDPOINT, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"❌ Erro ao obter previsão: {e}")
        return None


def obter_historico(limit=50):
    try:
        response = requests.get(f"{HISTORY_ENDPOINT}?limit={limit}", timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"❌ Erro ao obter histórico: {e}")
        return None


def enviar_para_thingsboard(dados):
    try:
        response = requests.post(
            TELEMETRY_ENDPOINT,
            json=dados,
            headers={"Content-Type": "application/json"},
            timeout=10
        )
        response.raise_for_status()
        return True
    except requests.exceptions.RequestException as e:
        print(f"❌ Erro ao enviar para ThingsBoard: {e}")
        return False


def formatar_dados_telemetria(previsao, historico=None):
    metricas = previsao.get("model_metrics", {})
    rmse = metricas.get("rmse", 0)
    mae = metricas.get("mae", 0)
    r2 = metricas.get("r2", 0)
    
    dados = {
        "temperatura_atual": previsao.get("current_temperature"),
        "temperatura_prevista": previsao.get("predicted_temperature"),
        "erro_rmse": round(rmse, 4) if rmse else 0,
        "erro_mae": round(mae, 4) if mae else 0,
        "r2_score": round(r2, 4) if r2 else 0,
        "erro_rmse_centesimos": round(rmse * 100, 2) if rmse else 0,
        "timestamp_previsao": previsao.get("prediction_time"),
    }

    if dados["temperatura_atual"] and dados["temperatura_prevista"]:
        dados["diferenca_prevista"] = round(
            dados["temperatura_prevista"] - dados["temperatura_atual"], 2
        )
    
    if historico and len(historico) > 0:
        ultimo = historico[-1]
        dados["umidade"] = ultimo.get("humidity")
        dados["vento"] = ultimo.get("wind_speed")
        dados["radiacao"] = ultimo.get("radiation")
        dados["precipitacao"] = ultimo.get("precipitation")
    
    return dados


def main():
    print("=" * 60)
    print("🚀 INTEGRAÇÃO FASTAPI → THINGSBOARD")
    print("=" * 60)
    print(f"📡 FastAPI: {FASTAPI_URL}")
    print(f"📊 ThingsBoard: {THINGSBOARD_URL}")
    print(f"⏱️  Intervalo: {INTERVALO_SEGUNDOS}s")
    print("=" * 60)
    print("\nIniciando envio de dados... (Ctrl+C para parar)\n")
    
    contador = 0
    
    while True:
        try:
            contador += 1
            timestamp = datetime.now().strftime("%H:%M:%S")
            previsao = obter_previsao()
            if not previsao:
                print(f"[{timestamp}] ⚠️  Não foi possível obter previsão")
                time.sleep(INTERVALO_SEGUNDOS)
                continue
            historico = obter_historico(limit=1)
            
            dados = formatar_dados_telemetria(previsao, historico)

            sucesso = enviar_para_thingsboard(dados)
            
            if sucesso:
                print(f"[{timestamp}] ✅ Envio #{contador}")
                print(f"           Atual: {dados['temperatura_atual']:.1f}°C")
                print(f"           Previsão T+1h: {dados['temperatura_prevista']:.1f}°C")
                print(f"           Δ: {dados.get('diferenca_prevista', 0):+.1f}°C")
            else:
                print(f"[{timestamp}] ❌ Falha no envio #{contador}")
            
            print()
            time.sleep(INTERVALO_SEGUNDOS)
            
        except KeyboardInterrupt:
            print("\n\n👋 Encerrando...")
            break
        except Exception as e:
            print(f"❌ Erro inesperado: {e}")
            time.sleep(INTERVALO_SEGUNDOS)


if __name__ == "__main__":
    main()
