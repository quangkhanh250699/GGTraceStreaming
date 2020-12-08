# docker network create sandbox-cluster &&\
docker-compose -p hadoop-cluster up -d &&\
docker run -it --rm --name hdfs-shell --network sandbox-cluster -e "CORE_CONF_fs_defaultFS=hdfs://hadoop-namenode:8020" -e "CLUSTER_NAME=hadoop-sandbox" -t uhopper/hadoop:2.7.2 /bin/bash
