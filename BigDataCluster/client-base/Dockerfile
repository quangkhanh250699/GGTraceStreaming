FROM ubuntu

ARG spark_version=3.0.1

RUN apt-get update -y && \
    apt-get install -y python3-pip &&\
    ln -s /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/* &&\
    cd / &&\
    apt-get update -y &&\
    apt-get install -y openjdk-8-jre-headless &&\
    apt-get install -y openjdk-8-jdk-headless &&\
    export JAVA_HOME=lib/jvm/java-1.8.0-openjdk-amd64 &&\
    export PATH=$JAVA_HOME/bin:$PATH &&\
    apt-get install -y curl &&\
    curl https://downloads.lightbend.com/scala/2.12.10/scala-2.12.10.deb -o /opt/scala_2.12.10.deb &&\
    dpkg -i /opt/scala_2.12.10.deb

CMD [ "bash" ]
