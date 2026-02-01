import requests
import os

# Configurações via Variáveis de Ambiente (Segurança)
TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

# Repositórios para monitorar
REPOS = [
    "backend-br/vagas",
    "frontendbr/vagas",
    "devops-br/vagas"
]

# Palavras-chave de Engenharia de Dados
KEYWORDS = ["data", "dados", "etl", "airflow", "spark", "dbt", "python", "sql"]

def buscar_vagas():
    vagas_encontradas = []
    for repo in REPOS:
        url = f"https://api.github.com/repos/{repo}/issues?state=open"
        response = requests.get(url)
        if response.status_code == 200:
            issues = response.json()
            for issue in issues:
                title = issue['title'].lower()
                # Verifica se alguma palavra-chave está no título
                if any(key in title for key in KEYWORDS):
                    vagas_encontradas.append(f"📍 {repo}\n🔗 {issue['html_url']}\n{issue['title']}\n")
    return vagas_encontradas

def enviar_telegram(mensagem):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": mensagem}
    requests.post(url, json=payload)

if __name__ == "__main__":
    vagas = buscar_vagas()
    if vagas:
        header = "🚀 Novas Vagas de Engenharia de Dados encontradas!\n\n"
        enviar_telegram(header + "\n".join(vagas))