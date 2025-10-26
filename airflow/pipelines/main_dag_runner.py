
import sys
import os
from dotenv import load_dotenv

# Adiciona a raiz do projeto ao sys.path dinamicamente
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
sys.path.append(ROOT_DIR)

# Carrega variáveis de ambiente
load_dotenv(dotenv_path=os.path.join(ROOT_DIR, '.env'))

from main import main

def run_pipeline():
    main()