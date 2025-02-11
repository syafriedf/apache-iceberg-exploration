FROM apache/airflow:2.5.1

USER root
WORKDIR /opt/airflow

RUN apt-get update && \
    apt-get install -y openjdk-11-jdk procps && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

RUN mkdir -p /opt/spark/jars && \
    curl -L https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.4_2.12/1.4.3/iceberg-spark-runtime-3.4_2.12-1.4.3.jar -o /opt/spark/jars/iceberg-spark-runtime.jar

RUN curl -L -o /opt/spark/jars/postgresql-42.2.18.jar https://repo1.maven.org/maven2/org/postgresql/postgresql/42.2.18/postgresql-42.2.18.jar

# RUN mkdir -p /opt/spark/jars && \
#     curl -L https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.4_2.12/1.4.3/iceberg-spark-runtime-3.4_2.12-1.4.3.jar \
#         -o /opt/spark/jars/iceberg-spark-runtime-3.4_2.12-1.4.3.jar

ARG SPARK_MAJOR_VERSION=3.5
ARG ICEBERG_VERSION=1.4.3
ARG ICEBERG_SPARK_SCALA="iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_2.12"
ARG JAR_PACKAGE="${ICEBERG_SPARK_SCALA}-${ICEBERG_VERSION}.jar"

# Download iceberg spark runtime
RUN curl https://repo1.maven.org/maven2/org/apache/iceberg/${ICEBERG_SPARK_SCALA}/${ICEBERG_VERSION}/${JAR_PACKAGE} -Lo /opt/spark/jars/${JAR_PACKAGE}

# Download Java AWS bundle
# RUN curl https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.17.257/bundle-2.17.257.jar -Lo /opt/spark/jars/bundle-2.17.257.jar
RUN curl -s https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar -Lo /opt/spark/jars/iceberg-aws-bundle-${ICEBERG_VERSION}.jar

# Add iceberg spark runtime jar to IJava classpath
ENV IJAVA_CLASSPATH=/opt/spark/jars/*

COPY requirements.txt /requirements.txt
COPY docker/entrypoint.sh /entrypoint.sh
COPY data/ ./data/
COPY dags/ /opt/airflow/dags/

RUN chmod +x /entrypoint.sh
RUN chown airflow: /requirements.txt /entrypoint.sh


USER airflow
RUN pip install --no-cache-dir -r /requirements.txt

USER root
RUN chown -R airflow: ${AIRFLOW_HOME}

USER airflow
ENTRYPOINT ["/entrypoint.sh"]
