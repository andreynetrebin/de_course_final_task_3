FROM apache/airflow:2.9.2

USER root

# Установка необходимых пакетов
RUN apt-get update && \
    apt install -y default-jdk wget && \
    apt-get autoremove -yqq --purge && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Установка Spark
ENV SPARK_VERSION=3.4.2
ENV SPARK_HOME=/opt/spark/spark-${SPARK_VERSION}

RUN if [ ! -d "${SPARK_HOME}" ]; then \
        wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
        mkdir -p ${SPARK_HOME} && \
        tar -xvf spark-${SPARK_VERSION}-bin-hadoop3.tgz -C /opt/spark --strip-components=1 && \
        rm spark-${SPARK_VERSION}-bin-hadoop3.tgz; \
    fi

# Скачивание JDBC-драйвера PostgreSQL
RUN wget https://jdbc.postgresql.org/download/postgresql-42.7.7.jar -P /opt/spark/jars/

# Скачивание JDBC-драйвера ClickHouse
RUN wget https://github.com/yandex/clickhouse-jdbc/releases/download/v0.3.2/clickhouse-jdbc-0.3.2.jar -P /opt/spark/jars/

# Настройка переменных окружения
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_HOME=/opt/spark/spark-3.4.2-bin-hadoop3
ENV PATH=$PATH:$JAVA_HOME/bin:$SPARK_HOME/bin
ENV PYTHONPATH=$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip

# Копирование и установка Python зависимостей
COPY requirements.txt /requirements.txt
RUN chmod 777 /requirements.txt

USER airflow
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r /requirements.txt