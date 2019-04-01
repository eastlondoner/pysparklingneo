from pyspark.sql import SparkSession

from .property_graph import PropertyGraph
from .neo4j_config import Neo4jConfig
from .util import helper

class SparkCypherSession(object):

    def __init__(self, spark_context):
        self.spark_session = SparkSession(spark_context)
        self._ctx = spark_context
        self._jvm = self.spark_session._jvm
        self._gateway = spark_context._gateway
        self._jSparkCypherSession = self._jvm.org.opencypher.spark.api.CAPSSession(
            self.spark_session._jsparkSession)

    def register_source(self, neo4jNamespace: str, neo4jConfig: Neo4jConfig):
        self._jSparkCypherSession.registerSource(
            neo4jNamespace,
            helper(self._ctx).neo4jGraphSource(self._jSparkCypherSession, neo4jConfig._jneo4jConfig)
        )

    def create_graph(self, nodes, relationships):
        jNodeDataFrames = [self._createJNodeDataFrame(node) for node in nodes]

        jRelationshipDataFrames = [self._createJRelationshipDataFrame(relationship) for relationship in relationships]

        jGraph = self._jSparkCypherSession.createGraph(self._toSeq(jNodeDataFrames),
                                                       self._toSeq(jRelationshipDataFrames))
        return PropertyGraph(jGraph, self)

    def cypher(self, query):
        return helper(self._ctx).runCAPSSessionCypher(self._jSparkCypherSession, query)

    def _createJNodeDataFrame(self, node):
        return self._jvm.org.apache.spark.graph.api.NodeDataFrame(
            node.df._jdf,
            node.idColumn,
            self._toSet(node.labels),
            self._toMap(node.properties),
            self._toMap(node.optionalLabels)
        )

    def _createJRelationshipDataFrame(self, relationship):
        return self._jvm.org.apache.spark.graph.api.RelationshipDataFrame(
            relationship.df._jdf,
            relationship.idColumn,
            relationship.sourceIdColumn,
            relationship.targetIdColumn,
            self._toSet(relationship.labels),
            self._toMap(relationship.properties)
        )

    def _toSet(self, set):
        return self._jvm.PythonUtils.toSet(set)

    def _toMap(self, map):
        return self._jvm.PythonUtils.toScalaMap(map)

    def _toSeq(self, seq):
        return self._jvm.PythonUtils.toSeq(seq)

    def _toList(self, list):
        return self._jvm.PythonUtils.toList(list)
