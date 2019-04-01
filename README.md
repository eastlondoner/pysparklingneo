# PysparklingNeo

A glittery package that brings joy to working with Neo4j from pyspark.

## Quick Start

In one terminal start spark & neo4j running:
```bash
./run-infrastructure.sh
# ctrl+c to stop spark & neo docker containers
```
n.b. neo4j data is persisted between runs in ./docker-spark/neodata/

Then in another terminal:
```bash
# Build the pyspark-neo spark package
./package.sh

# Run a pyspark job (assumes you have spark & neo running already)
./pyspark.sh pyspark-jobs/count-first-names.py
```

## Dependencies

### Python
 - virtualenv

### Java/Scala
 - scala 2.12
 - sbt
 


## Contents

### pyspark-neo4j

Is a Spark package, mostly written in python, which provides the ability to construct RDDs (and hopefully soon DataFrames) from Neo4j.

```python
read_conf = pyspark_neo4j.ReadConf(uri="bolt://neo4j:7687", auth=("neo4j", "graphsinspark"))
my_table_from_neo = sc.neo4jTable("MATCH (n:Person) RETURN properties(n) as n", read_conf=read_conf)
```

You have to build and package pyspark-neo4j before you can use it. To build use make:

```bash
cd pyspark-neo4j
make dist
# the spark-package jar is now located at ./pyspark-neo4j/target/scala-2.11/pyspark-neo4j-assembly-0.0.1.jar
```

pyspark-neo4j is inspired by pyspark-cassandra.

### docker-spark

Is a docker-compose project for running a mini spark cluster alongside a neo4j database to exercise pyspark-neo4j

To start spark & neo4j: 
```bash
cd docker-spark
docker-compose up
# ctrl+c to stop
```

To run a pyspark job
```bash

```

docker-spark is based on gettyimages/docker-spark
