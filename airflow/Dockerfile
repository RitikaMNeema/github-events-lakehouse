FROM apache/airflow:2.10.5-python3.11

# Spark needs a JVM. procps gives us `ps`, which Spark's scripts call.
USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends default-jdk procps curl \
 && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Create the Ivy cache WITH the right ownership before switching user.
# Spark resolves spark.jars.packages into here at runtime, and a fresh
# named volume mounted at this path inherits the image directory's
# ownership -- so without this the volume lands root-owned and Ivy fails
# with FileNotFoundException on its own resolution cache.
RUN mkdir -p /home/airflow/.ivy2/cache /home/airflow/.ivy2/jars \
 && chown -R airflow:root /home/airflow/.ivy2

USER airflow
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Maven coordinates are resolved once into a named volume (spark-ivy) so
# repeated job runs don't re-download the Iceberg jars.
ENV PYTHONPATH=/opt/project
WORKDIR /opt/project
