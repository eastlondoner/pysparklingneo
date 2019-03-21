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

"""
    This module provides python support for Apache Spark's Resillient
    Distributed Datasets from Apache neo4j CQL rows using the Spark
    neo4j Connector from:
    https://github.com/datastax/spark-neo4j-connector.
"""

import inspect

import pyspark.rdd

from .conf import ReadConf
from .context import Neo4jSparkContext, monkey_patch_sc
from .rdd import saveToNeo4j, joinWithNeo4jTable, deleteFromNeo4j
from .types import Row, UDT

__all__ = [
    "Neo4jSparkContext",
    "ReadConf",
    "Row",
    "UDT",
]

# Monkey patch the default python RDD so that it can be stored to neo4j as
# CQL rows

pyspark.rdd.RDD.saveToNeo4j = saveToNeo4j
pyspark.rdd.RDD.joinWithNeo4jTable = joinWithNeo4jTable
pyspark.rdd.RDD.deleteFromNeo4j = deleteFromNeo4j

# Monkey patch the sc variable in the caller if any
frame = inspect.currentframe().f_back
# Go back at most 10 frames
for _ in range(10):
    if not frame:
        break
    elif "sc" in frame.f_globals:
        monkey_patch_sc(frame.f_globals["sc"])
        break
    else:
        frame = frame.f_back

__author__ = 'Andrew Jefferson'
__email__ = 'andy@octavian.ai'
__version__ = '0.0.1'
