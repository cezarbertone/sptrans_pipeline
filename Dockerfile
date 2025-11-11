# Imagem base leve com Python 3.11
FROM python:3.11-slim

# Instala bash e dependências do sistema (necessário para dbt e Postgres)
RUN apt-get update && apt-get install -y \
    bash \
    git \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Define diretório de trabalho
WORKDIR /app

# Copia requirements e instala dependências
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia todo o código do app
COPY . .

# Variáveis de ambiente para integração com MinIO e Postgres
ENV MINIO_ENDPOINT=minio:9000 \
    MINIO_ROOT_USER=minioadmin \
    MINIO_ROOT_PASSWORD=minioadmin \
    DB_HOST=sptrans_db \
    DB_PORT=5432 \
    DB_NAME=sptrans \
    DB_USER=postgres \
    DB_PASSWORD=postgres

# Comando padrão (pode ser alterado para ENTRYPOINT se necessário)
CMD ["bash"]
# Para rodar o app diretamente, descomente:
# ENTRYPOINT ["python", "main.py"]