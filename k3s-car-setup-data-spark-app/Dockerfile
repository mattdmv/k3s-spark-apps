FROM docker.stackable.tech/stackable/spark-k8s:3.5.1-stackable24.7.0

ENV PYTHONPATH "app:app/utils:app/car_setup_data_job:$PYTHONPATH"

RUN curl --fail -o /stackable/spark/jars/iceberg-spark-runtime-3.5_2.12-1.5.0.jar "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.0/iceberg-spark-runtime-3.5_2.12-1.5.0.jar"
RUN curl --fail -o /stackable/spark/jars/bundle-2.20.131.jar "https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.20.131/bundle-2.20.131.jar"
RUN curl --fail -o /stackable/spark/jars/url-connection-client-2.20.131.jar "https://repo1.maven.org/maven2/software/amazon/awssdk/url-connection-client/2.20.131/url-connection-client-2.20.131.jar"
RUN curl --fail -o /stackable/spark/jars/hadoop-aws-3.3.4.jar "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"

WORKDIR /my_app

COPY requirements.txt .
COPY app/ app/

RUN pip install -r requirements.txt