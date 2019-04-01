#!/usr/bin/env bash

set -e

DOCKER_IMAGE_NAME=mysparkimage

pushd docker-spark
  docker build . -t "${DOCKER_IMAGE_NAME}"
  cat docker-compose.yml.template | DOCKER_IMAGE_NAME="${DOCKER_IMAGE_NAME}" envsubst  > docker-compose.yml
  docker-compose up
popd


exit 0
spark-submit \
	--jars /path/to/pyspark-cassandra-assembly-<version>.jar \
	--py-files /path/to/pyspark-cassandra-assembly-<version>.jar \
	--conf spark.cassandra.connection.host=your,cassandra,node,names \
	--master spark://spark-master:7077 \
	yourscript.py