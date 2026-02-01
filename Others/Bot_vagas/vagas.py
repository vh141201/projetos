import requests
import os
from dotenv import load_dotenv

# Tenta carregar o .env apenas se o arquivo existir (uso local)
load_dotenv()

# Prioriza as variáveis de ambiente (Secrets do GitHub)
TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

REPOS = [
    "backend-br/vagas",
    "frontendbr/vagas",
    "devops-br/vagas"
]

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
                if any(key in title for key in KEYWORDS):
                    vagas_encontradas.append(f"📍 {repo}\n🔗 {issue['html_url']}\n{issue['title']}\n")
    return vagas_encontradas

def enviar_telegram(mensagem):
    # Verificação de segurança para não quebrar sem as chaves
    if not TELEGRAM_TOKEN or not CHAT_ID:
        print("Erro: Chaves do Telegram não encontradas!")
        return
    
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": mensagem}
    requests.post(url, json=payload)

if __name__ == "__main__":
    vagas = buscar_vagas()
    if vagas:
        header = "🚀 Novas Vagas encontradas!\n\n"
        enviar_telegram(header + "\n".join(vagas))
        print("Mensagem enviada para o Telegram!")
    else:
        print("Script rodou, mas nenhuma vaga foi encontrada com as palavras-chave.")