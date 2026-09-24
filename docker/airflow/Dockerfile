FROM apache/airflow:2.7.1-python3.10

USER root

# Install OpenJDK 17 for PySpark, and build-essential for C/C++ compilation (needed for some packages)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

# Install python requirements
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
