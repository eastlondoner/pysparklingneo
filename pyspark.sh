#!/usr/bin/env bash

# Copy the python scripts into the mounted dist dir
cp -r ./pyspark-jobs ./docker-spark/dist/

# Copy pyspark
cp ./pyspark-neo4j/target/scala-2.11/pyspark-neo4j-assembly-0.0.1.jar ./docker-spark/dist/

docker exec -it docker-spark_master_1 /bin/bash -c "spark-submit \
	--jars /dist/pyspark-neo4j-assembly-0.0.1.jar \
	--py-files /dist/pyspark-neo4j-assembly-0.0.1.jar \
	'/dist/$1'"
