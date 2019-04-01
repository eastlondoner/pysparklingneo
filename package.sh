#!/usr/bin/env bash

set -e

docker run -it --rm -v "${HOME}/.m2":/root/.m2 -v "${HOME}/.ivy2":/root/.ivy2 -v "$(pwd)/pyspark-neo4j":/pyspark-neo4j --workdir /pyspark-neo4j hseeberger/scala-sbt bash -c "apt-get update && apt-get install -y make zip python3 python3-pip && rm /usr/bin/python && ln -s /usr/bin/python3 /usr/bin/python && pip3 install virtualenv && make dist"
