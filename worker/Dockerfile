FROM python:3.12-slim

WORKDIR /app

# Copia a lista de bibliotecas primeiro
COPY requirements.txt .

# Instala as bibliotecas
RUN pip install --no-cache-dir -r requirements.txt

# Copia o seu código
COPY worker.py .

# Roda o Worker no modo unbuffered (-u) para os prints aparecerem na AWS
CMD ["python", "-u", "worker.py"]