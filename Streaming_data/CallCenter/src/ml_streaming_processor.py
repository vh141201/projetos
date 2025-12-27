import json
import pandas as pd
import numpy as np
from confluent_kafka import Consumer, KafkaError
from joblib import load
from sklearn.metrics import confusion_matrix, classification_report

# CARREGA MODELOS E ESCALONADOR 
try:
    knn = load('../models/knn_model.joblib')
    regressao_log = load('../models/reglog_model.joblib')
    random_forest = load('../models/randomforest_model.joblib')
    scaler = load('../models/scaler.joblib')
    print("Modelos e scaler carregados.")

except Exception as e:
    print(f"Erro ao carregar: {e}")
    exit()

# CONFIGURAÇÃO DO CONSUMIDOR KAFKA
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'detector_fraude_group',
    'auto.offset.reset': 'latest'
}

consumer = Consumer(**conf)
consumer.subscribe(['chamadas-telefonicas'])

# Listas para armazenar os resultados
y_true = []
y_pred_knn = []
y_pred_rf = []
y_pred_rl = []

print("Detector ativo. Aguardando chamadas.")

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"Erro no Kafka: {msg.error()}")
                break

        # PROCESSA A MENSAGEM
        dados_chamada = json.loads(msg.value().decode('utf-8'))
        
        # DataFrame para evitar UserWarning
        features_df = pd.DataFrame([{
            'duration_sec': dados_chamada['duration_sec'],
            'patience_sec': dados_chamada['patience_sec'],
            'country_risk_score': dados_chamada['country_risk_score'],
            'is_international': dados_chamada['is_international']
        }])

        # Normalização
        features_scaled = scaler.transform(features_df)
        
        # PREDIÇÕES DOS TRÊS ALGORITMOS
        pred_knn = knn.predict(features_scaled)[0]
        pred_rf = random_forest.predict(features_scaled)[0]
        pred_rl = regressao_log.predict(features_scaled)[0]

        # ARMAZENAMENTO PARA MÉTRICAS
        if 'is_fraud_real' in dados_chamada:
            y_true.append(dados_chamada['is_fraud_real'])
            y_pred_knn.append(pred_knn)
            y_pred_rf.append(pred_rf)
            y_pred_rl.append(pred_rl)

        print(f"Chamada {dados_chamada['call_id']}, é fraude? {dados_chamada['is_fraud_real']}, Predito -> KNN:{pred_knn} RF:{pred_rf} RL:{pred_rl}")

except KeyboardInterrupt:
    print("\n" + "="*50)
    print("MATRIZES DE CONFUSÃO")
    print("="*50)
    
    modelos = [
        ("KNN", y_pred_knn),
        ("Random Forest", y_pred_rf),
        ("Regressão Logística", y_pred_rl)
    ]

    for nome, y_pred in modelos:
        if len(y_true) > 0:
            print(f"\nMODELO: {nome}")
            print("Matriz de Confusão:")
            print(confusion_matrix(y_true, y_pred))
            print("\nRelatório de Classificação:")
            print(classification_report(y_true, y_pred))
            print("-" * 30)

finally:
    consumer.close()