from .cypher_result import CypherResult
from pyspark.sql import DataFrame


class PropertyGraph(object):

    def __init__(self, _jPropertyGraph, _sparkCypherSession):
        self._jPropertyGraph = _jPropertyGraph
        self._sparkCypherSession = _sparkCypherSession
        self._sqlContext = _sparkCypherSession.sparkSession._wrapped

    def cypher(self, query):
        jCypherResult = self._jPropertyGraph.cypher(query)
        return CypherResult(jCypherResult, self._sparkCypherSession)

    def nodes(self):
        jDf = self._jPropertyGraph.nodes
        return DataFrame(jDf, self._sqlContext)

    def vertices(self):
        return self.nodes()

    def relationships(self):
        jDf = self._jPropertyGraph.relationships
        return DataFrame(jDf, self._sqlContext)

    def edges(self):
        return self.relationships()