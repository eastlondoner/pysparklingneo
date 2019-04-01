#!/usr/bin/env bash

set -e

# Copy the python scripts into the mounted dist dir
cp -r ./pyspark-jobs ./docker-spark/dist/

# Copy pyspark
cp ./pyspark-neo4j/target/scala-2.12/pyspark-neo4j-assembly-0.0.1.jar ./docker-spark/dist/

if [[ -z "$1" ]]; then
    echo "No python script to run provided. Usage: ./pyspark.sh pyspark-jobs/my-script.py "
    exit 1
fi

docker exec -it docker-spark_master_1 /bin/bash -c "spark-submit \
	--jars /dist/pyspark-neo4j-assembly-0.0.1.jar \
	--py-files /dist/pyspark-neo4j-assembly-0.0.1.jar \
	--conf 'spark.jars=/dist/*.jar' \
	'/dist/$1'"
