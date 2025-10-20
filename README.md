
# SPTrans Pipeline

Este projeto implementa um pipeline de dados que coleta informações da API Olho Vivo da SPTrans, processa os dados com Python e os armazena em um banco de dados PostgreSQL. A orquestração é feita com Apache Airflow, e o ambiente é totalmente containerizado com Docker Compose.

## 🚀 Visão Geral da Arquitetura

## 🧰 Tecnologias Utilizadas

- Python 3.11
- Pandas
- Requests
- SQLAlchemy
- Psycopg2
- PostgreSQL 15
- PgAdmin 4
- Apache Airflow 2.7.1
- Docker & Docker Compose

## 📁 Estrutura do Projeto

```
sptrans_pipeline/
├── api/
│   ├── autenticacao.py
│   └── consulta_linhas_zona_sul.py
├── main.py
├── requirements.txt
├── .env
├── airflow/
│   └── dags/
│       ├── main_dag_runner.py
│       └── sptrans_dag.py
└── docker-compose.yml
```

## ⚙️ Como Executar

1. Clone o repositório:
```bash
git clone git@github.com:wellingtonpawlino/sptrans_pipeline.git
cd sptrans_pipeline
```

2. Configure o arquivo `.env` com as variáveis de conexão:
```env
DB_HOST=db
DB_PORT=5432
DB_NAME=sptrans
DB_USER=postgres
DB_PASSWORD=postgres
```

3. Construa e inicie os containers:
```bash
docker-compose up --build -d
```

4. Acesse os serviços:
- Airflow: [http://localhost:8080](http://localhost:8080)
- PgAdmin: [http://localhost:5050](http://localhost:5050)

## 📅 Agendamento com Airflow

O DAG `sptrans_pipeline_dag` é executado diariamente às 4h da manhã e chama o script principal que coleta e salva os dados da Zona Sul de São Paulo.

## 🗃️ Banco de Dados

Os dados são armazenados na tabela `linhas_zona_sul` com os seguintes campos:
- `cl`: código da linha
- `lc`: circular
- `lt`: número da linha
- `sl`: sentido
- `tp`: tipo

## 📌 Observações

- O projeto utiliza `.dockerignore` para evitar conflitos e otimizar o build.
- O Airflow é executado em containers separados e não precisa estar no `requirements.txt` da aplicação.

## 👨‍💻 Autores

Wellington Santos

