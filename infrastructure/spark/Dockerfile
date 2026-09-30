# =============================================================================
# Spark + PySpark Docker Image for Dinamo ETL Platform
# Includes: Kafka client libs, Avro support, S3A connector, Delta Lake
# /dinamo_streaming/infrastructure/spark/Dockerfile
# =============================================================================
FROM apache/spark:3.4.1
USER root

# --- System dependencies ---
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl wget netcat-openbsd procps jq \
    && rm -rf /var/lib/apt/lists/*


# --- Build dependencies para Python 3.11 ---
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    zlib1g-dev \
    libncurses5-dev \
    libgdbm-dev \
    libnss3-dev \
    libssl-dev \
    libreadline-dev \
    libffi-dev \
    libsqlite3-dev \
    wget \
    libbz2-dev \
    && rm -rf /var/lib/apt/lists/*

# --- Instala Python 3.11 ---
RUN wget https://www.python.org/ftp/python/3.11.9/Python-3.11.9.tgz && \
    tar -xf Python-3.11.9.tgz && \
    cd Python-3.11.9 && \
    ./configure --enable-optimizations && \
    make -j$(nproc) && \
    make altinstall && \
    cd .. && \
    rm -rf Python-3.11.9 Python-3.11.9.tgz

# --- Define Python 3.11 como padrão ---
RUN ln -sf /usr/local/bin/python3.11 /usr/bin/python3 && \
    ln -sf /usr/local/bin/pip3.11 /usr/bin/pip

# --- PySpark usando Python 3.11 ---
ENV PYSPARK_PYTHON=/usr/bin/python3
ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3

ENV PATH="/opt/spark/bin:${PATH}"

# --- Python dependencies ---
COPY requirements.txt /tmp/requirements.txt

RUN pip install --upgrade pip setuptools wheel

RUN pip install -i https://pypi.org/simple/ --default-timeout=120 --retries=3 -r /tmp/requirements.txt && rm /tmp/requirements.txt

RUN ln -s /opt/spark/python/lib/py4j-0.10.9.7-src.zip \
    /opt/spark/python/lib/py4j.zip

ENV PYTHONPATH="/opt/spark/python:/opt/spark/python/lib/py4j.zip:/opt/spark/app/src"

# --- Spark JARs: Kafka, Avro, S3A, Delta ---
ENV SPARK_JARS_DIR=/opt/spark/jars

# Kafka Structured Streaming connector
RUN curl -fSL "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.4.1/spark-sql-kafka-0-10_2.12-3.4.1.jar" \
    -o ${SPARK_JARS_DIR}/spark-sql-kafka-0-10_2.12-3.4.1.jar

# Kafka clients
RUN curl -fSL "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.6.1/kafka-clients-3.6.1.jar" \
    -o ${SPARK_JARS_DIR}/kafka-clients-3.6.1.jar

# Spark Avro
RUN curl -fSL "https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.12/3.4.1/spark-avro_2.12-3.4.1.jar" \
    -o ${SPARK_JARS_DIR}/spark-avro_2.12-3.4.1.jar

# Spark token provider for Kafka
RUN curl -fSL "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.4.1/spark-token-provider-kafka-0-10_2.12-3.4.1.jar" \
    -o ${SPARK_JARS_DIR}/spark-token-provider-kafka-0-10_2.12-3.4.1.jar

# Commons pool (Kafka dependency)
RUN curl -fSL "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar" \
    -o ${SPARK_JARS_DIR}/commons-pool2-2.12.0.jar

# Delta Lake
RUN curl -fSL "https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar" \
    -o ${SPARK_JARS_DIR}/delta-core_2.12-2.4.0.jar

RUN curl -fSL "https://repo1.maven.org/maven2/io/delta/delta-storage/2.4.0/delta-storage-2.4.0.jar" \
    -o ${SPARK_JARS_DIR}/delta-storage-2.4.0.jar

# Hadoop AWS (S3A filesystem)
RUN curl -fSL "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" \
    -o ${SPARK_JARS_DIR}/hadoop-aws-3.3.6.jar
RUN curl -fSL "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.630/aws-java-sdk-bundle-1.12.630.jar" \
    -o ${SPARK_JARS_DIR}/aws-java-sdk-bundle-1.12.630.jar


# --- Application code mount point ---
RUN mkdir -p /opt/spark/app/src /opt/spark/checkpoints /opt/spark/data-lake

WORKDIR /opt/spark/app

COPY infrastructure/spark/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY src/ /opt/spark/app/src/

RUN chown -R 1000:1000 /opt/spark/app /opt/spark/checkpoints /opt/spark/data-lake

# USER 1001
COPY src/main.py /opt/spark/app/main.py
