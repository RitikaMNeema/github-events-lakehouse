FROM apache/airflow:2.10.5-python3.11

# Spark needs a JVM. procps gives us `ps`, which Spark's scripts call.
USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends default-jdk procps curl \
 && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Maven coordinates are resolved once into a named volume (spark-ivy) so
# repeated job runs don't re-download the Iceberg jars.
ENV PYTHONPATH=/opt/project
WORKDIR /opt/project
