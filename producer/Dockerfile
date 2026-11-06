FROM python:3.14-slim

WORKDIR /app
RUN pip install --no-cache-dir kafka-python==2.2.15
COPY producer/producer.py /app/producer.py

CMD ["python", "-u", "producer.py"]
