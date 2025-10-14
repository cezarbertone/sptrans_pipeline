
import requests
from config.config import TOKEN


def autenticar():
    session = requests.Session()
    url_login = f"http://api.olhovivo.sptrans.com.br/v2.1/Login/Autenticar?token={TOKEN}"
    response = session.post(url_login)

    if response.status_code == 200 and response.text.lower() == "true":
        print("✅ Autenticação bem-sucedida!")
        return session
    else:
        print("❌ Falha na autenticação.")
        print("Status:", response.status_code)
        print("Resposta:", response.text)
