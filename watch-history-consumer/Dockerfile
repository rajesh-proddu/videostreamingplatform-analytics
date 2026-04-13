FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ src/

ENV KAFKA_BROKERS=localhost:9092
ENV KAFKA_WATCH_TOPIC=watch-events
ENV KAFKA_GROUP_ID=watch-history-consumer
ENV ICEBERG_CATALOG_NAME=glue
ENV ICEBERG_WAREHOUSE=s3://iceberg-warehouse/
ENV ICEBERG_TABLE=analytics.watch_history
ENV AWS_REGION=us-east-1

USER nobody

CMD ["python", "-m", "src.consumer"]
