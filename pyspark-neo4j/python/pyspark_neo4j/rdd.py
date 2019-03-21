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

import sys
from copy import copy

from pyspark.rdd import RDD

from pyspark_neo4j.conf import ReadConf, WriteConf
from pyspark_neo4j.format import ColumnSelector
from pyspark_neo4j.util import as_java_array, as_java_object, helper

if sys.version_info > (3,):
    long = int  # @ReservedAssignment


def saveToNeo4j(rdd, keyspace=None, table=None, columns=None,
                row_format=None, keyed=None,
                write_conf=None, **write_conf_kwargs):
    """
        Saves an RDD to Neo4j. The RDD is expected to contain dicts with
        keys mapping to CQL columns.

        Arguments:
        @param rdd(RDD):
            The RDD to save. Equals to self when invoking saveToNeo4j on a
            monkey patched RDD.
        @param keyspace(string):in
            The keyspace to save the RDD in. If not given and the rdd is a
            Neo4jRDD the same keyspace is used.
        @param table(string):
            The CQL table to save the RDD in. If not given and the rdd is a
            Neo4jRDD the same table is used.

        Keyword arguments:
        @param columns(iterable):
            The columns to save, i.e. which keys to take from the dicts in the
            RDD. If None given all columns are be stored.

        @param row_format(RowFormat):
            Make explicit how to map the RDD elements into Neo4j rows.
            If None given the mapping is auto-detected as far as possible.
        @param keyed(bool):
            Make explicit that the RDD consists of key, value tuples (and not
            arrays of length two).

        @param write_conf(WriteConf):
            A WriteConf object to use when saving to Neo4j
        @param **write_conf_kwargs:
            WriteConf parameters to use when saving to Neo4j
    """

    keyspace = keyspace or getattr(rdd, 'keyspace', None)
    if not keyspace:
        raise ValueError("keyspace not set")

    table = table or getattr(rdd, 'table', None)
    if not table:
        raise ValueError("table not set")

    # create write config as map
    write_conf = WriteConf.build(write_conf, **write_conf_kwargs)
    write_conf = as_java_object(rdd.ctx._gateway, write_conf.settings())

    if isinstance(columns, dict):
        # convert the columns to a map where the value is the
        # action inside Neo4j
        columns = as_java_object(rdd.ctx._gateway, columns) if columns else None
    else:
        # convert the columns to a string array
        columns = as_java_array(rdd.ctx._gateway, "String",
                                columns) if columns else None

    helper(rdd.ctx) \
        .saveToNeo4j(
        rdd._jrdd,
        keyspace,
        table,
        columns,
        row_format,
        keyed,
        write_conf,
    )


def deleteFromNeo4j(rdd, keyspace=None, table=None, deleteColumns=None,
                    keyColumns=None, row_format=None, keyed=None,
                    write_conf=None, **write_conf_kwargs):
    """
        Delete data from Neo4j table, using data from the RDD as primary
        keys. Uses the specified column names.

        Arguments:
        @param rdd(RDD):
            The RDD to save. Equals to self when invoking saveToNeo4j on a
            monkey patched RDD.
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

    keyspace = keyspace or getattr(rdd, 'keyspace', None)
    if not keyspace:
        raise ValueError("keyspace not set")

    table = table or getattr(rdd, 'table', None)
    if not table:
        raise ValueError("table not set")

    # create write config as map
    write_conf = WriteConf.build(write_conf, **write_conf_kwargs)
    write_conf = as_java_object(rdd.ctx._gateway, write_conf.settings())

    # convert the columns to a string array
    deleteColumns = as_java_array(rdd.ctx._gateway, "String", deleteColumns) \
        if deleteColumns else None
    keyColumns = as_java_array(rdd.ctx._gateway, "String", keyColumns) \
        if keyColumns else None

    helper(rdd.ctx) \
        .deleteFromNeo4j(
        rdd._jrdd,
        keyspace,
        table,
        deleteColumns,
        keyColumns,
        row_format,
        keyed,
        write_conf,
    )


# this jrdd is for compatibility with pyspark.rdd.RDD
# while allowing this constructor to be use for type checking etc
# and setting _jrdd //after// invoking this constructor
class DummyJRDD(object):
    def id(self):
        return -1


class _Neo4jRDD(RDD):
    """
        A Resilient Distributed Dataset of Neo4j rows. As any RDD,
        objects of this class are immutable; i.e. operations on this RDD
        generate a new RDD.
    """

    def __init__(self, ctx, graph_name="graph.db", read_conf=None,
                 **read_conf_kwargs):

        self.graph_name = graph_name
        self.read_conf = ReadConf.build(read_conf, **read_conf_kwargs)
        self._limit = None
        self._crdd = None

        jrdd = DummyJRDD()

        super(_Neo4jRDD, self).__init__(jrdd, ctx)

    @property
    def _helper(self):
        return helper(self.ctx)

    def get_jrdd(self):
        return self._crdd._jrdd if self._crdd else DummyJRDD()

    def set_jrdd(self, jrdd):
        if not isinstance(jrdd, DummyJRDD):
            raise Exception("cannot override jrdd")
        self._id = jrdd.id

    _jrdd = property(get_jrdd, set_jrdd)

    def get_crdd(self):
        return self._crdd

    def set_crdd(self, crdd):
        self._crdd = crdd
        self._id = self._jrdd.id

    crdd = property(get_crdd, set_crdd)

    saveToNeo4j = saveToNeo4j
    deleteFromNeo4j = deleteFromNeo4j

    def select(self, *columns):
        """Creates a Neo4jRDD with the select clause applied."""
        columns = as_java_array(self.ctx._gateway, "String",
                                (str(c) for c in columns))
        return self._specialize('select', columns)

    def where(self, clause, *args):
        """Creates a Neo4jRDD with a CQL where clause applied.
        @param clause: The where clause, either complete or with ? markers
        @param *args: The parameters for the ? markers in the where clause.
        """
        args = as_java_array(self.ctx._gateway, "Object", args)
        return self._specialize('where', *[clause, args])

    def take(self, num):
        if self._limit == num:
            return self.crdd.take(num)
        return self.limit(num).take(num)

    def collect(self):
        """
        Return a list that contains all of the elements in this RDD.

        .. note:: This method should only be used if the resulting array is
        expected to be small, as all the data is loaded into the driver's
        memory.
        """
        if self._limit:
            return self.take(self._limit)
        else:
            return super(_Neo4jRDD, self).collect()

    def neo4jCount(self):
        """Lets Neo4j perform a count, instead of loading data to Spark"""
        return self._crdd.neo4jCount()

    def _specialize(self, func_name, *args, **kwargs):
        func = getattr(self._helper, func_name)

        new = copy(self)
        new.crdd = func(new._crdd, *args, **kwargs)

        return new

    def spanBy(self, *columns):
        """"Groups rows by the given columns without shuffling.

        @param *columns: an iterable of columns by which to group.

        Note that:
        -    The rows are grouped by comparing the given columns in order and
            starting a new group whenever the value of the given columns
            changes.

            This works well with using the partition keys and one or more of
            the clustering keys. Use rdd.groupBy(...) for any other grouping.
        -    The grouping is applied on the partition level. I.e. any grouping
            will be a subset of its containing partition.
        """

        return SpanningRDD(self.ctx, self._crdd, self._jrdd, self._helper,
                           columns)

    def __copy__(self):
        c = self.__class__.__new__(self.__class__)
        c.__dict__.update(self.__dict__)
        return c


def joinWithNeo4jTable(left_rdd, keyspace, table):
    """
        Join an RDD with a Neo4j table on the partition key. Use .on(...)
        to specifiy other columns to join on. .select(...), .where(...) and
        .limit(...) can be used as well.

        Arguments:
        @param left_rdd(RDD):
            The RDD to join. Equals to self when invoking
            joinWithNeo4jTable on a monkey patched RDD.
        @param keyspace(string):
            The keyspace to join on
        @param table(string):
            The CQL table to join on.
    """

    return Neo4jJoinRDD(left_rdd, keyspace, table)


def run_cypher(read_conf, cypher):
    from neo4j import GraphDatabase
    driver = GraphDatabase.driver(read_conf.uri, auth=(read_conf.auth_username, read_conf.auth_password))

    with driver.session(access_mode='READ') as session:
        for record in session.run(cypher).records():
            data = record.data()
            # flatten results for the special case where only one is returned
            if len(data) == 1:
                for k, v in data.items():
                    yield v
            else:
                yield data


class Neo4jCypherRDD(_Neo4jRDD):
    def __init__(self, ctx, cypher, graph_name="graph.db", read_parallelism=1, partition_key=None, read_conf=None,
                 **read_conf_kwargs):
        super(Neo4jCypherRDD, self).__init__(ctx, graph_name, read_conf,
                                             **read_conf_kwargs)

        if read_parallelism > 1:
            if not partition_key:
                raise Exception("partition_key must be set if using read parallelism")
        self._read_parallelism = read_parallelism
        self._partition_key = partition_key
        if self._partition_key is not None:
            raise Exception("partitioning not implemented")
        self._key_by = ColumnSelector.none()

        self._jread_conf = as_java_object(ctx._gateway, self.read_conf.settings())

        # TODO: move the tracking of the cypher into java

        # TODO: repartition
        self._cypher = cypher
        self._crdd = self._build_crdd()

    def _build_crdd(self):
        cypher = str(self._cypher)
        rc = self.read_conf
        return self.ctx.parallelize(range(self._read_parallelism)).mapPartitions(lambda x: run_cypher(rc, cypher))

    def by_primary_key(self):
        return self.key_by(primary_key=True)

    def key_by(self, primary_key=True, partition_key=False, *columns):
        # TODO implement keying by arbitrary columns
        if columns:
            raise NotImplementedError(
                'keying by arbitrary columns is not (yet) supported')
        if partition_key:
            raise NotImplementedError(
                'keying by partition key is not (yet) supported')

        new = copy(self)
        new._key_by = ColumnSelector(partition_key, primary_key, *columns)
        new.crdd = self.crdd

        return new

    # TODO: this is a hack we should move this to a specialization in Java
    def limit(self, limit):
        """Creates a new Neo4jRDD with the limit clause applied."""
        if self._limit is not None and self._limit < limit:
            # TODO: warn if user does this?
            limit = self._limit
        new_cypher = self._cypher + " LIMIT " + str(limit)
        new_rdd = Neo4jCypherRDD(self.ctx, new_cypher, graph_name=self.graph_name,
                                 read_parallelism=self._read_parallelism, partition_key=self._partition_key,
                                 read_conf=self.read_conf)
        new_rdd._limit = limit
        return new_rdd


class Neo4jJoinRDD(_Neo4jRDD):
    """
        TODO
    """

    def __init__(self, left_rdd, keyspace, table):
        super(Neo4jJoinRDD, self).__init__(left_rdd.ctx, keyspace, table)
        self.crdd = self._helper \
            .joinWithNeo4jTable(left_rdd._jrdd, keyspace, table)

    def on(self, *columns):
        columns = as_java_array(self.ctx._gateway, "String",
                                (str(c) for c in columns))
        return self._specialize('on', columns)
