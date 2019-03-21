#!/usr/bin/env bash

pushd docker-spark
  docker-compose up
popd


exit 0
spark-submit \
	--jars /path/to/pyspark-cassandra-assembly-<version>.jar \
	--py-files /path/to/pyspark-cassandra-assembly-<version>.jar \
	--conf spark.cassandra.connection.host=your,cassandra,node,names \
	--master spark://spark-master:7077 \
	yourscript.py