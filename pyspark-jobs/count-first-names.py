from pyspark import SparkContext

sc = SparkContext("spark://master:7077", "App Name")
sc.setLogLevel('WARN')

# import pyspark_neo4j after importing SparkContext
import pyspark_neo4j

print("it begins")

read_conf = pyspark_neo4j.ReadConf(uri="bolt://neo4j:7687", auth=("neo4j", "graphsinspark"))
my_table_from_neo = sc.neo4jTable("MATCH (n:Person) RETURN properties(n) as n", read_conf=read_conf)

sample = my_table_from_neo.take(10)
if len(sample) == 0:
    print("There are no People in the graph. "
          "You need to load the movies dataset into neo4j first. "
          "Open neo4j in the browser (http://localhost:7474) and run :play movies to load the movies data set.")
    exit(1)
else:
    print("A sample of people from the dataset:", sample)


def add_first_name(node):
    name = node.get("name", None)
    if name:
        node["first_name"] = name.split(" ")[0]
    return node


my_table_from_neo = my_table_from_neo.map(add_first_name)
print(my_table_from_neo.take(10))

keyed_by_name = my_table_from_neo.map(lambda n: (n["first_name"], n))
count_by_name = keyed_by_name.groupByKey().mapValues(len)
print(count_by_name.takeOrdered(10, key=lambda x: -x[1]))

print("the end")
