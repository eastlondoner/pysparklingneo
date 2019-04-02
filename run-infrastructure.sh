#!/usr/bin/env bash

set -e

SPARK_DOCKER_IMAGE=mysparkimage
JUPYTER_DOCKER_IMAGE_NAME=myjupyterimage

pushd docker-spark
  docker build . -t "${SPARK_DOCKER_IMAGE}"
popd
pushd docker-jupyter
  docker build --build-arg SPARK_DOCKER_IMAGE="${SPARK_DOCKER_IMAGE}" . -t "${JUPYTER_DOCKER_IMAGE_NAME}"
popd

pushd docker-spark
  cat docker-compose.yml.template | JUPYTER_DOCKER_IMAGE_NAME="${JUPYTER_DOCKER_IMAGE_NAME}" SPARK_DOCKER_IMAGE="${SPARK_DOCKER_IMAGE}" envsubst  > docker-compose.yml
  docker-compose up
popd


exit 0
spark-submit \
	--jars /path/to/pyspark-cassandra-assembly-<version>.jar \
	--py-files /path/to/pyspark-cassandra-assembly-<version>.jar \
	--conf spark.cassandra.connection.host=your,cassandra,node,names \
	--master spark://spark-master:7077 \
	yourscript.py