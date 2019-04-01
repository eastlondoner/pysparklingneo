# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from functools import partial

import pyspark.context
from py4j.java_gateway import java_import

from .rdd import Neo4jCypherRDD
from .neo4j_config import Neo4jConfig

def monkey_patch_sc(sc):
    print("monkey patching")
    sc.__class__ = Neo4jSparkContext
    sc.__dict__["neo4jTable"] = partial(
        Neo4jSparkContext.neo4jTable, sc)
    sc.__dict__["neo4jTable"].__doc__ = \
        Neo4jSparkContext.neo4jTable.__doc__

    java_import(sc._gateway.jvm, "org.opencypher.spark.api.io.*")
    java_import(sc._gateway.jvm, "org.opencypher.spark.api.CAPSSession")
    java_import(sc._gateway.jvm, "org.opencypher.okapi.neo4j.io.Neo4jConfig")
    java_import(sc._gateway.jvm, "org.opencypher.spark.api.GraphSources")

    sc.__dict__["neo4jConfig"] = partial(
        Neo4jSparkContext.neo4jConfig, sc)
    sc.__dict__["neo4jConfig"].__doc__ = \
        Neo4jSparkContext.neo4jConfig.__doc__

class Neo4jSparkContext(pyspark.context.SparkContext):
    """Wraps a SparkContext which allows reading from neo4j"""

    def neo4jTable(self, cypher, *args, **kwargs):
        """Returns a Neo4jCypherRDD for the given keyspace and table"""
        return Neo4jCypherRDD(self, cypher, *args, **kwargs)

    def neo4jConfig(self, uri, username, password, encrypted=True):
        return Neo4jConfig(self, uri, username, password, encrypted)