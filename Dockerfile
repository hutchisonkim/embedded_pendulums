# Start with an official TensorFlow Serving image
FROM gcr.io/tfx-oss-public/tfx:1.14.0

# Set the working directory inside the container
WORKDIR /app

# Install system dependencies for MySQL
RUN apt-get update --allow-releaseinfo-change && apt-get install -y \
    libmysqlclient-dev \
    libssl-dev \
    python3-dev \
    build-essential \
    gcc \
    g++ \
    netcat \
    && rm -rf /var/lib/apt/lists/*

# Clean up unnecessary files to reduce image size
RUN apt-get clean

# Install SQL related Python dependencies
RUN pip install mysqlclient
RUN pip install tfx

ENV MYSQLCLIENT_CFLAGS="-I/usr/include/mysql"
ENV MYSQLCLIENT_LDFLAGS="-L/usr/lib/mysql"

RUN pip install PyMySQL
RUN pip install apache-airflow[pymysql]
RUN pip install tfx[airflow]

RUN pip install apache-airflow-providers-cncf-kubernetes
RUN pip install apache-airflow-providers-apache-kafka
RUN pip install kafka-python
RUN pip install pyspark
RUN pip install google-cloud-storage
RUN pip install requests
RUN pip install pillow
RUN pip install numpy
RUN pip install geopandas
RUN pip install osmnx
RUN pip install scikit-learn
RUN pip install matplotlib
RUN pip install psycopg2-binary

# Verify Java installation for PySpark (if needed)
RUN apt-get update && apt-get install -y openjdk-11-jdk && apt-get clean

# Install Graphviz for generating DAG images
RUN apt-get update && apt-get install -y graphviz
RUN pip install graphviz

# Additional environment variables for Spark compatibility
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Ensure the Airflow configuration points to PyMySQL for MySQL
COPY airflow.cfg /opt/airflow/airflow.cfg
COPY ./dags /opt/airflow/dags

# Expose the port for the Airflow UI
EXPOSE 8080

# Use an entrypoint script to initialize and start Airflow
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]