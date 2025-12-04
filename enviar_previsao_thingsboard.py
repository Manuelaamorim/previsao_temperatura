"""
Script para enviar previsões da FastAPI para o ThingsBoard.

Este script:
1. Chama o endpoint /predict/auto da FastAPI
2. Envia os dados (temperatura atual + previsão) para o ThingsBoard
3. Executa periodicamente para manter o dashboard atualizado

Uso:
    python enviar_previsao_thingsboard.py
"""

import requests
import time
import json
from datetime import datetime

# =============================================================================
# CONFIGURAÇÕES
# =============================================================================

# FastAPI
FASTAPI_URL = "http://localhost:8000"
PREDICT_ENDPOINT = f"{FASTAPI_URL}/predict/auto"
HISTORY_ENDPOINT = f"{FASTAPI_URL}/data/history"

# ThingsBoard
THINGSBOARD_URL = "http://localhost:9090"
# Token do dispositivo (você pode precisar ajustar isso)
# Este é o token de acesso do dispositivo no ThingsBoard
DEVICE_ACCESS_TOKEN = "zb4uP0BTV7zGsvPnu9IG"  # Token padrão do simulador
TELEMETRY_ENDPOINT = f"{THINGSBOARD_URL}/api/v1/{DEVICE_ACCESS_TOKEN}/telemetry"

# Intervalo entre envios (segundos)
INTERVALO_SEGUNDOS = 30

# =============================================================================
# FUNÇÕES
# =============================================================================

def obter_previsao():
    """Obtém a previsão da FastAPI."""
    try:
        response = requests.get(PREDICT_ENDPOINT, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"❌ Erro ao obter previsão: {e}")
        return None


def obter_historico(limit=50):
    """Obtém histórico de dados da FastAPI."""
    try:
        response = requests.get(f"{HISTORY_ENDPOINT}?limit={limit}", timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"❌ Erro ao obter histórico: {e}")
        return None


def enviar_para_thingsboard(dados):
    """Envia dados de telemetria para o ThingsBoard."""
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
    """Formata os dados para envio ao ThingsBoard."""
    
    # Extrair métricas do modelo
    metricas = previsao.get("model_metrics", {})
    rmse = metricas.get("rmse", 0)
    mae = metricas.get("mae", 0)
    r2 = metricas.get("r2", 0)
    
    dados = {
        "temperatura_atual": previsao.get("current_temperature"),
        "temperatura_prevista": previsao.get("predicted_temperature"),
        # Arredondar para 4 casas decimais para garantir que não seja 0
        "erro_rmse": round(rmse, 4) if rmse else 0,
        "erro_mae": round(mae, 4) if mae else 0,
        "r2_score": round(r2, 4) if r2 else 0,
        # Também enviar em centésimos de grau para melhor visualização
        "erro_rmse_centesimos": round(rmse * 100, 2) if rmse else 0,  # Ex: 0.11 -> 11 centésimos
        "timestamp_previsao": previsao.get("prediction_time"),
    }
    
    # Calcular diferença entre previsão e atual
    if dados["temperatura_atual"] and dados["temperatura_prevista"]:
        dados["diferenca_prevista"] = round(
            dados["temperatura_prevista"] - dados["temperatura_atual"], 2
        )
    
    # Adicionar dados históricos mais recentes se disponível
    if historico and len(historico) > 0:
        ultimo = historico[-1]
        dados["umidade"] = ultimo.get("humidity")
        dados["vento"] = ultimo.get("wind_speed")
        dados["radiacao"] = ultimo.get("radiation")
        dados["precipitacao"] = ultimo.get("precipitation")
    
    return dados


def main():
    """Loop principal de envio de dados."""
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
            
            # 1. Obter previsão
            previsao = obter_previsao()
            if not previsao:
                print(f"[{timestamp}] ⚠️  Não foi possível obter previsão")
                time.sleep(INTERVALO_SEGUNDOS)
                continue
            
            # 2. Obter histórico (opcional)
            historico = obter_historico(limit=1)
            
            # 3. Formatar dados
            dados = formatar_dados_telemetria(previsao, historico)
            
            # 4. Enviar para ThingsBoard
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
