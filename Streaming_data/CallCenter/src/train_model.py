############################################################################################
# Aqui serão gerados os dados usados para treinar os modelos de previsão
# Usei algoritmos como KNN, random forest e regressão logística
# Os dados gerados aqui serão direcionados de forma que hajam correlações entre os dados
# Ex.: Um país com country_risk_score alto tem mais chance de ser golpe, ou uma ligação
# nacional ter menos chance de ser golpe, etc.
# A proporção média de golpe é de 20%
############################################################################################
import os
import pandas as pd
import numpy as np
from sklearn.neighbors import KNeighborsClassifier
from sklearn.preprocessing import StandardScaler
from joblib import dump
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier

base_path = os.path.dirname(__file__)
target_folder = os.path.abspath(os.path.join(base_path, '..', 'models'))

folder_path = target_folder

if not os.path.exists(folder_path):
    os.makedirs(folder_path)

# Configurações de limites
d_h, d_l = 360, 10
c_m, c_l = 0.6, 0.3
p_l, p_h = 4, 11

def train_and_save_model():
    n = 5000
    prob_scam = 0.20 

    df = pd.DataFrame({
        'duration_sec': np.random.randint(d_l, d_h, n),
        'patience_sec': np.random.uniform(p_l, p_h, n),
        'country_risk_score': np.random.uniform(c_l, c_m, n),
        'is_international': np.random.choice([0, 1], n, p=[0.8, 0.2])
    })

    df['is_scam'] = np.random.choice([0, 1], size=n, p=[1-prob_scam, prob_scam])

    mask_scam = (df['is_scam'] == 1)
    n_scams = mask_scam.sum()

    # Aplicação de correlações para o modelo aprender padrões reais
    df.loc[mask_scam, 'country_risk_score'] = np.random.uniform(0.7, 0.95, n_scams)
    df.loc[mask_scam, 'is_international'] = np.random.choice([0, 1], n_scams, p=[0.30, 0.70])
    df.loc[mask_scam, 'duration_sec'] = np.random.randint(5, 40, n_scams)

    features = ['duration_sec', 'patience_sec', 'country_risk_score', 'is_international']
    X = df[features]
    y = df['is_scam']

    scaler = StandardScaler()
    X_norm = scaler.fit_transform(X)

    # Treinando os modelos
    knn = KNeighborsClassifier(n_neighbors=3) # K pequeno geralmente resulta em maior sensibilidade
    knn.fit(X_norm, y)
    regressao_log = LogisticRegression()
    regressao_log.fit(X_norm, y)
    random_forest = RandomForestClassifier(n_estimators=100)
    random_forest.fit(X_norm, y)


    dump(knn, os.path.join(folder_path, 'knn_model.joblib'))
    dump(regressao_log, os.path.join(folder_path, 'reglog_model.joblib'))
    dump(random_forest, os.path.join(folder_path, 'randomforest_model.joblib'))
    dump(scaler, os.path.join(folder_path, 'scaler.joblib'))

    print(f"Treinamento finalizado. Proporção real de golpes: {df['is_scam'].mean()*100:.1f}%")

if __name__ == "__main__":
    train_and_save_model()