FROM python:3.11-slim

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

# (opcional) garantir que o diretório de saída exista
RUN mkdir -p data

CMD ["python", "main.py"]