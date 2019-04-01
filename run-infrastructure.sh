#!/usr/bin/env bash

set -e

DOCKER_IMAGE_NAME=mysparkimage

pushd docker-spark
  docker build . -t "${DOCKER_IMAGE_NAME}"
  DOCKER_IMAGE_NAME="${DOCKER_IMAGE_NAME}" yq '.services |= with_entries(if .key == "neo4j" then . else .value.image |= env.DOCKER_IMAGE_NAME end)' docker-compose.yml.template > docker-compose.yml
  docker-compose up
popd


exit 0
spark-submit \
	--jars /path/to/pyspark-cassandra-assembly-<version>.jar \
	--py-files /path/to/pyspark-cassandra-assembly-<version>.jar \
	--conf spark.cassandra.connection.host=your,cassandra,node,names \
	--master spark://spark-master:7077 \
	yourscript.py