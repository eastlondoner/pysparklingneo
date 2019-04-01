from pyspark import SparkContext

sc = SparkContext("spark://master:7077", "App Name")
sc.setLogLevel('WARN')

# import pyspark_neo4j after importing SparkContext
import pyspark_neo4j

print("it begins")

caps = pyspark_neo4j.SparkCypherSession(sc)

uri = "bolt://neo4j:7687"
auth = ("neo4j", "graphsinspark")
neo4j_config = sc.neo4jConfig(uri, auth[0], auth[1], encrypted=False)

neo4j_session = None
driver = None
try:
    driver = neo4j_config.driver()

    try:
        neo4j_session = driver.session()
    finally:
        if neo4j_session:
            neo4j_session.close()
finally:
    if driver:
        driver.close()

print("I have functioning Neo4jConfig in python")

my_namespace = "mynamespace"
caps.register_source(my_namespace, neo4j_config)

print("I have registered my neo4j graph source in python")

print("Try and run cypher against neo4j...")
my_df = caps.cypher(
    "FROM GRAPH mynamespace.graph "
    "MATCH (n:Person) RETURN n"
)

my_df.show(pyspark_neo4j.util.helper(caps._ctx).printOptions())

from pyspark.sql.functions import udf

def get_first_name(s):
    return s.split(' ')[0]

get_first_name_udf = udf(get_first_name)

print(my_df.rdd.take(5))
my_df.withColumn("firstName", get_first_name_udf("name")).show()


print("the end")
