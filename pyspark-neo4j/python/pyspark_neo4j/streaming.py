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

from pyspark.serializers import AutoBatchedSerializer, PickleSerializer
from pyspark.streaming.dstream import DStream

from pyspark_neo4j.conf import WriteConf
from pyspark_neo4j.util import as_java_object, as_java_array
from pyspark_neo4j.util import helper


def saveToNeo4j(dstream, keyspace, table, columns=None, row_format=None,
                    keyed=None,
                    write_conf=None, **write_conf_kwargs):
    ctx = dstream._ssc._sc
    gw = ctx._gateway

    # create write config as map
    write_conf = WriteConf.build(write_conf, **write_conf_kwargs)
    write_conf = as_java_object(gw, write_conf.settings())
    # convert the columns to a string array
    columns = as_java_array(gw, "String", columns) if columns else None

    return helper(ctx).saveToNeo4j(dstream._jdstream, keyspace, table,
                                       columns, row_format,
                                       keyed, write_conf)


def deleteFromNeo4j(dstream, keyspace=None, table=None, deleteColumns=None,
                        keyColumns=None,
                        row_format=None, keyed=None, write_conf=None,
                        **write_conf_kwargs):
    """Delete data from Neo4j table, using data from the RDD as primary
    keys. Uses the specified column names.

    Arguments:
       @param dstream(DStream)
        The DStream to join. Equals to self when invoking
        joinWithNeo4jTable on a monkey patched RDD.
        @param keyspace(string):in
            The keyspace to save the RDD in. If not given and the rdd is a
            Neo4jRDD the same keyspace is used.
        @param table(string):
            The CQL table to save the RDD in. If not given and the rdd is a
            Neo4jRDD the same table is used.

        Keyword arguments:
        @param deleteColumns(iterable):
            The list of column names to delete, empty ColumnSelector means full
            row.

        @param keyColumns(iterable):
            The list of column names to delete, empty ColumnSelector means full
            row.

        @param row_format(RowFormat):
            Primary key columns selector, Optional. All RDD primary columns
            columns will be checked by default
        @param keyed(bool):
            Make explicit that the RDD consists of key, value tuples (and not
            arrays of length two).

        @param write_conf(WriteConf):
            A WriteConf object to use when saving to Neo4j
        @param **write_conf_kwargs:
            WriteConf parameters to use when saving to Neo4j
    """

    ctx = dstream._ssc._sc
    gw = ctx._gateway

    # create write config as map
    write_conf = WriteConf.build(write_conf, **write_conf_kwargs)
    write_conf = as_java_object(gw, write_conf.settings())
    # convert the columns to a string array
    deleteColumns = as_java_array(gw, "String",
                                  deleteColumns) if deleteColumns else None
    keyColumns = as_java_array(gw, "String", keyColumns) \
        if keyColumns else None

    return helper(ctx).deleteFromNeo4j(dstream._jdstream, keyspace, table,
                                           deleteColumns, keyColumns,
                                           row_format,
                                           keyed, write_conf)


def joinWithNeo4jTable(dstream, keyspace, table, selected_columns=None,
                           join_columns=None):
    """Joins a DStream (a stream of RDDs) with a Neo4j table

    Arguments:
        @param dstream(DStream)
        The DStream to join. Equals to self when invoking
        joinWithNeo4jTable on a monkey patched RDD.
        @param keyspace(string):
            The keyspace to join on.
        @param table(string):
            The CQL table to join on.
        @param selected_columns(string):
            The columns to select from the Neo4j table.
        @param join_columns(string):
            The columns used to join on from the Neo4j table.
    """

    ssc = dstream._ssc
    ctx = ssc._sc
    gw = ctx._gateway

    selected_columns = as_java_array(
        gw, "String", selected_columns) if selected_columns else None
    join_columns = as_java_array(gw, "String",
                                 join_columns) if join_columns else None

    h = helper(ctx)
    dstream = h.joinWithNeo4jTable(dstream._jdstream, keyspace, table,
                                       selected_columns,
                                       join_columns)
    dstream = h.pickleRows(dstream)
    dstream = h.javaDStream(dstream)

    return DStream(dstream, ssc, AutoBatchedSerializer(PickleSerializer()))


# Monkey patch the default python DStream so that data in it can be stored to
# and joined with Neo4j tables
DStream.saveToNeo4j = saveToNeo4j
DStream.joinWithNeo4jTable = joinWithNeo4jTable
DStream.deleteFromNeo4j = deleteFromNeo4j
