FROM client-base

ARG spark_version=3.0.1
ARG hadoop_version=2.7

RUN apt-get update -y && \
    curl https://downloads.apache.org/spark/spark-${spark_version}/spark-${spark_version}-bin-hadoop${hadoop_version}.tgz -o spark.tgz && \
    tar -xf spark.tgz && \
    mv spark-${spark_version}-bin-hadoop${hadoop_version} /usr/bin/ && \
    mkdir /usr/bin/spark-${spark_version}-bin-hadoop${hadoop_version}/logs && \
    rm spark.tgz &&\
    pip3 install pyspark==${spark_version} &&\
    pip3 install pyspark-stubs &&\
    pip3 install findspark &&\
    pip3 install kafka-python &&\
    pip3 install msgpack &&\
    pip3 install jupyterlab &&\
    mkdir -p /opt/workspace

ENV SPARK_HOME /usr/bin/spark-${spark_version}-bin-hadoop${hadoop_version}

EXPOSE 8888

CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", "--NotebookApp.token="]
