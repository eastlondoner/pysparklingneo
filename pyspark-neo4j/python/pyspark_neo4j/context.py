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
from .rdd import Neo4jCypherRDD

def monkey_patch_sc(sc):
    sc.__class__ = Neo4jSparkContext
    sc.__dict__["neo4jTable"] = partial(
        Neo4jSparkContext.neo4jTable, sc)
    sc.__dict__["neo4jTable"].__doc__ = \
        Neo4jSparkContext.neo4jTable.__doc__


class Neo4jSparkContext(pyspark.context.SparkContext):
    """Wraps a SparkContext which allows reading from neo4j"""

    def neo4jTable(self, cypher, *args, **kwargs):
        """Returns a Neo4jCypherRDD for the given keyspace and table"""
        return Neo4jCypherRDD(self, cypher, *args, **kwargs)
