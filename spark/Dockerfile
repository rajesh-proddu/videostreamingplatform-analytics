FROM apache/spark-py:3.5.3

USER root

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/

ENTRYPOINT ["spark-submit", "--packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3", "src/watch_history_ingest.py"]
