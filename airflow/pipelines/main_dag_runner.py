

import sys
sys.path.append('/app/sptrans_pipeline')

from main_dag_runner import run_pipeline

from dotenv import load_dotenv

# Adiciona a raiz do projeto ao sys.path
sys.path.append('/app')

# Carrega variáveis de ambiente
load_dotenv(dotenv_path='/app/.env')

from main import main  # importa a função principal do seu pipeline

def run_pipeline():
    main()