# Usamos a mesma imagem leve
FROM python:3.12-slim

WORKDIR /app

# Instala as dependências necessárias para o Worker (S3 e PostgreSQL)
RUN pip install --no-cache-dir boto3 psycopg2-binary

COPY worker.py /app/worker.py

# Inicia o script em loop
CMD ["python", "-u", "worker.py"]