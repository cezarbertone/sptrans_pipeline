
# SPTrans Pipeline

Este projeto implementa um pipeline para coleta, processamento e análise de dados da 
**API Olho Vivo** da SPTrans, permitindo monitoramento em tempo real da frota de ônibus da cidade de São Paulo.

---

## 📌 Objetivo
Automatizar a ingestão e transformação dos dados fornecidos pela SPTrans, possibilitando análises sobre:
- Localização dos veículos
- Linhas e itinerários
- Status operacional

---

## ⚙️ Tecnologias Utilizadas
- **Python 3.x**
- **Pandas** para manipulação de dados
- **Requests** para integração com a API
- **Airflow** (para orquestrar, os processo)
- **Docker** para containerização
- **PostgreSQL** 

---


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

## 🛠️ Conexão com o Banco via PgAdmin

| Campo                 | Valor      |
|----------------------|------------|
| **Host name/address**| `db`       |
| **Port**             | `5432`     |
| **Maintenance database** | `sptrans` |
| **Username**         | `postgres` |
| **Password**         | `postgres` |
``
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

Wellington Santos | Cézar Tadeu

