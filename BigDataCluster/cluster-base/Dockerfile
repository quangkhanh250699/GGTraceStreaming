ARG debian_buster_image_tag=8-jre-slim
FROM openjdk:${debian_buster_image_tag}

ARG shared_workspace=/opt/workspace

RUN mkdir -p ${shared_workspace} && \
    apt-get update -y && \
    apt-get install -y python3 && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

ENV SHARED_WORKSPACE=${shared_workspace}

VOLUME ${shared_workspace}
CMD ["bash"]
