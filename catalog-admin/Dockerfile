FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY admin.py .

USER nobody

ENTRYPOINT ["python", "admin.py"]
CMD ["create-tables"]
