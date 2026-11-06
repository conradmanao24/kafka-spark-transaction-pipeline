FROM python:3.14-slim

WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends default-jre-headless procps && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir pyspark==4.2.0
RUN pip install --no-cache-dir psycopg2-binary==2.9.11

ENV SPARK_HOME=/usr/local/lib/python3.14/site-packages/pyspark
ENV PATH="${SPARK_HOME}/bin:${PATH}"
RUN mkdir -p /app/data /app/checkpoint /opt/kafka-jars
