import requests
import os
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

# Configuração de busca no Google
# Buscamos por Engenheiro de Dados no Brasil postadas nas últimas 24h
GOOGLE_SEARCH_URL = "https://www.google.com/search?q=vagas+engenheiro+de+dados+brasil&ibp=htl;jobs"

def buscar_vagas_github():
    repos = ["backend-br/vagas", "devops-br/vagas"]
    keywords = ["data", "dados", "etl", "airflow", "spark", "dbt", "python", "sql"]
    vagas = []
    
    for repo in repos:
        url = f"https://api.github.com/repos/{repo}/issues?state=open"
        response = requests.get(url)
        if response.status_code == 200:
            for issue in response.json():
                title = issue['title'].lower()
                if any(key in title for key in keywords):
                    vagas.append(f"🐙 GitHub: {issue['title']}\n🔗 {issue['html_url']}")
    return vagas

def buscar_vagas_google():
    # Simulamos um navegador para o Google não nos bloquear
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    # O Google Jobs é complexo para raspar puramente, 
    # então aqui enviamos o link da busca filtrada para você abrir
    link_direto = "https://www.google.com/search?q=vagas+engenheiro+de+dados+brasil&ibp=htl;jobs"
    return [f"🔍 Google Jobs (Agregador):\nConfira as vagas de hoje: \n🔗 {link_direto}"]

def enviar_telegram(mensagem):
    if not TELEGRAM_TOKEN or not CHAT_ID:
        print("Erro: Chaves não encontradas!")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    requests.post(url, json={"chat_id": CHAT_ID, "text": mensagem})

if __name__ == "__main__":
    vagas_gh = buscar_vagas_github()
    vagas_gg = buscar_vagas_google()
    
    total_vagas = vagas_gh + vagas_gg
    
    if total_vagas:
        header = "🚀 Monitor de Vagas de Dados Atualizado!\n\n"
        enviar_telegram(header + "\n\n---\n\n".join(total_vagas))
        print("Sucesso! Mensagem enviada.")
    else:
        print("Nenhuma vaga encontrada.")