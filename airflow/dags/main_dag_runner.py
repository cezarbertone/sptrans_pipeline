import sys
import os
from dotenv import load_dotenv

# Adiciona a raiz do projeto ao sys.path
sys.path.append('/app')

# Carrega variáveis de ambiente
load_dotenv(dotenv_path='/app/.env')

from main import main  # importa a função principal do seu pipeline

def run_pipeline():
    main()