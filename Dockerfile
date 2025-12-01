FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p data/inmet_raw data/inmet_etl_bronze

ENV PYTHONUNBUFFERED=1

CMD ["python", "medalhao/main.py"]