FROM apache/airflow:2.9.1

# Switch to root user to install system dependencies (Java)
USER root
RUN apt-get update && \
    apt-get install -y default-jre-headless && \
    apt-get clean

# Switch back to the airflow user to install Python packages
USER airflow
RUN pip install --no-cache-dir pyspark==3.5.1
