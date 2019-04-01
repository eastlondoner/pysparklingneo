#!/usr/bin/env bash

set -e

docker run --rm -v "${HOME}/.m2":/root/.m2 -v "${HOME}/.ivy2":/root/.ivy2 -v "$(pwd)/pyspark-neo4j":/pyspark-neo4j --workdir /pyspark-neo4j hseeberger/scala-sbt bash -c "apt-get install -y make zip && make dist"
