import time
import random
import json
from confluent_kafka import Producer

# Como estamos simulando dados não sabemos se algo é ou não é um golpe,
# temos apenas as Features que seriam observadas no mundo real.

# --- CONFIGURAÇÃO KAFKA ---
conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(**conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"Erro: {err}")
    else:
        print(f"Sucesso. Tópico: {msg.topic()} | Partição: {msg.partition()}")

# Variáveis globais para criação de registros de dados
class Globals:
    a_h, a_m, a_l = 1.8, 1.35, 1.0
    d_h, d_l = 360, 5
    c_h, c_m, c_l = 0.8, 0.5, 0.3
    p_l, p_h = 4, 11

# Essa função gera os dados das chamadas
# 20% dos dados é forçado a ser um golpe, fiz isso para testar se o modelo tem capacidade 
# de decidir se a chamada é real ou não.

def generate_call_data(call_id):
    interval = random.triangular(high=Globals.a_h, low=Globals.a_l, mode=Globals.a_m)
    duration = random.randint(Globals.d_l, Globals.d_h)
    is_international = random.choice([True, False])
    country_risk_score = random.triangular(high=Globals.c_h, low=Globals.c_l, mode=Globals.c_m)
    paciencia_s = random.randint(Globals.p_l, Globals.p_h)

    # Lógica para simular a "Verdade" (Ground Truth) de 20%
    is_fraud_real = 0
    if random.random() < 0.20:
        is_fraud_real = 1
        # Força características de golpe para validar o modelo
        duration = random.randint(5, 40)
        country_risk_score = random.uniform(0.7, 0.95)

    return {
        "call_id": call_id,
        "is_fraud_real": is_fraud_real,
        "timestamp": time.time(),
        "arrival_interval": interval, 
        "duration_sec": duration,
        "is_international": int(is_international),
        "country_risk_score": country_risk_score,
        "patience_sec": paciencia_s,
    }


# Essa função envia os dados para um consumidor no Apache Kafka
def run_real_time_generator():
    call_counter = 1
    try:
        while True:
            data = generate_call_data(call_counter)
            payload = json.dumps(data).encode('utf-8')
            producer.produce('chamadas-telefonicas', value=payload, callback=delivery_report)
            producer.poll(0) 
            print(f"ENVIADO: ID {call_counter} | Real: {data['is_fraud_real']}")
            call_counter += 1
            time.sleep(data['arrival_interval']) 
    except KeyboardInterrupt:
        print("Finalizando...")

if __name__ == "__main__":
    run_real_time_generator()