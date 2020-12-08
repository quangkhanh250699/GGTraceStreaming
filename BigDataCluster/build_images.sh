# -- Software Stack Version

export spark_version="3.0.1"

# -- Building the Images

docker build \
  -f cluster-base/Dockerfile \
  -t cluster-base .

docker build \
  -f client-base/Dockerfile \
  -t client-base .

docker build \
  --build-arg spark_version=${spark_version} \
  -f spark-base/Dockerfile \
  -t spark-base .

docker build \
  -f spark-master/Dockerfile \
  -t spark-master .

docker build \
  -f spark-worker/Dockerfile \
  -t spark-worker .

docker build \
  --build-arg spark_version="${spark_version}" \
  -f qk-client/Dockerfile \
  -t qk-client .

