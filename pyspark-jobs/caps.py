from pyspark import SparkContext
from pyspark_neo4j import SparkCypherSession

sc = SparkContext("local", "App Name")
sc.setLogLevel('WARN')

# import pyspark_neo4j after importing SparkContext
import pyspark_neo4j

print("it begins")

SparkCypherSession(sc)

print("the end")
