FROM apache/spark:3.5.0

# Switch to root to install packages
USER root

# Install Python system tools
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    rm -rf /var/lib/apt/lists/*

# Copy requirements file
COPY requirements.txt /tmp/requirements.txt

# Install Python dependencies globally
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

# ---------------------------------------------------------------------------
# DOWNLOAD JARS
# ---------------------------------------------------------------------------
ENV ICEBERG_VERSION=1.5.0
ENV NESSIE_VERSION=0.82.0
ENV AWS_SDK_VERSION=1.12.262

RUN curl -o $SPARK_HOME/jars/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar

RUN curl -o $SPARK_HOME/jars/nessie-spark-extensions-3.5_2.12-${NESSIE_VERSION}.jar \
    https://repo1.maven.org/maven2/org/projectnessie/nessie-integrations/nessie-spark-extensions-3.5_2.12/${NESSIE_VERSION}/nessie-spark-extensions-3.5_2.12-${NESSIE_VERSION}.jar

RUN curl -o $SPARK_HOME/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar
RUN curl -o $SPARK_HOME/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

# ---------------------------------------------------------------------------
# FIX PERMISSIONS
# ---------------------------------------------------------------------------
# Create the home directory explicitly (because it doesn't exist)
RUN mkdir -p /home/spark

# Give the 'spark' user ownership of it
RUN chown -R spark:spark /home/spark

# Switch back to the default user for runtime
USER spark
WORKDIR /home/spark