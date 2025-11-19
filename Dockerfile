# Usar imagem oficial Python
FROM python:3.11-slim

# Definir diretório de trabalho
WORKDIR /app

# Copiar requirements e instalar dependências
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar todo o projeto
COPY . .

# Garantir que a pasta data existe
RUN mkdir -p data/inmet_raw data/inmet_etl_bronze

# Definir variável de ambiente (caso não venha pelo compose)
ENV PYTHONUNBUFFERED=1

# Comando padrão (pode ser substituído no docker-compose)
CMD ["python", "main.py"]