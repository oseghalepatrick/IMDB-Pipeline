# First-time build can take upto 10 mins.

FROM apache/airflow:2.2.3

ENV AIRFLOW_HOME=/opt/airflow

USER root
RUN apt-get update -qq && apt-get install vim -qqq

ENV JAVA_HOME=/home/spark/jdk-11.0.2
ENV PATH="${JAVA_HOME}/bin:${PATH}"

RUN JAVA_URL="https://download.java.net/java/GA/jdk11/9/GPL/openjdk-11.0.2_linux-x64_bin.tar.gz" \
    && TMP_DIR="$(mktemp -d)" \
    && curl -fL "${JAVA_URL}" --output "${TMP_DIR}/openjdk-11.0.2_linux-x64_bin.tar.gz" \
    && mkdir -p "/home/spark/" \
    && tar xzfv "${TMP_DIR}/openjdk-11.0.2_linux-x64_bin.tar.gz" -C "/home/spark/" \
    && rm -rf "${TMP_DIR}" \
    && java --version

ENV SPARK_HOME=/home/spark/spark-3.0.3-bin-hadoop3.2
ENV PATH="${SPARK_HOME}/bin:${PATH}"

RUN SPARK_URL="https://dlcdn.apache.org/spark/spark-3.0.3/spark-3.0.3-bin-hadoop3.2.tgz" \
    && TMP_DIR="$(mktemp -d)" \
    && curl -fL "${SPARK_URL}" --output "${TMP_DIR}/spark-3.0.3-bin-hadoop3.2.tgz" \
    # && mkdir -p "/home/spark/" \
    && tar xzfv "${TMP_DIR}/spark-3.0.3-bin-hadoop3.2.tgz" -C "/home/spark/" \
    && rm -rf "${TMP_DIR}" \
    && which pyspark \
    && ls "/home/spark/"

ENV PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
ENV PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9-src.zip:$PYTHONPATH"


# git gcc g++ -qqq

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Ref: https://airflow.apache.org/docs/docker-stack/recipes.html

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

ARG CLOUD_SDK_VERSION=322.0.0
ENV GCLOUD_HOME=/home/google-cloud-sdk

ENV PATH="${GCLOUD_HOME}/bin/:${PATH}"


RUN DOWNLOAD_URL="https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz" \
    && TMP_DIR="$(mktemp -d)" \
    && curl -fL "${DOWNLOAD_URL}" --output "${TMP_DIR}/google-cloud-sdk.tar.gz" \
    && mkdir -p "${GCLOUD_HOME}" \
    && tar xzf "${TMP_DIR}/google-cloud-sdk.tar.gz" -C "${GCLOUD_HOME}" --strip-components=1 \
    && "${GCLOUD_HOME}/install.sh" \
       --bash-completion=false \
       --path-update=false \
       --usage-reporting=false \
       --quiet \
    && rm -rf "${TMP_DIR}" \
    && gcloud --version



WORKDIR $AIRFLOW_HOME

COPY scripts scripts
RUN chmod +x scripts

USER $AIRFLOW_UID