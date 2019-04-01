/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package pyspark_neo4j

import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.api.java.JavaDStream
import org.apache.spark.streaming.dstream.DStream
import org.opencypher.okapi.neo4j.io.Neo4jConfig
import org.opencypher.spark.api.{CAPSSession, GraphSources}
import org.opencypher.spark.api.io.neo4j.Neo4jPropertyGraphDataSource
import org.opencypher.okapi.relational.api.planning.RelationalCypherResult
import org.opencypher.spark.impl.table.SparkTable.DataFrameTable
import org.opencypher.okapi.impl.util.PrintOptions

@SerialVersionUID(1L)
class PythonHelper() extends Serializable {


  /* ----------------------------------------------------------------------- */
  /* loading from neo4j ---------------------------------------------------- */
  /* ----------------------------------------------------------------------- */

  def neo4jGraphSource(
             caps: CAPSSession,
             config: Neo4jConfig,
           ): Neo4jPropertyGraphDataSource = {
    implicit val session: CAPSSession = caps
    return GraphSources.cypher.neo4j(config)
  }


  /* ----------------------------------------------------------------------- */
  /* do cypher ------------------------------------------------------------- */
  /* ----------------------------------------------------------------------- */

  // TODO: figure out if this can use type parameters
  def runCAPSSessionCypher(
                        caps: CAPSSession,
                        query: String,
                      ): RelationalCypherResult[DataFrameTable] = {
    return caps.cypher(query)
  }


  /* ----------------------------------------------------------------------- */
  /* span by columns ------------------------------------------------------- */
  /* ----------------------------------------------------------------------- */

  /* ----------------------------------------------------------------------- */
  /* save to cassandra ----------------------------------------------------- */
  /* ----------------------------------------------------------------------- */

  /* rdds ------------------------------------------------------------------ */


  /* dstreams -------------------------------------------------------------- */


  /* ----------------------------------------------------------------------- */
  /* join with cassandra tables -------------------------------------------- */
  /* ----------------------------------------------------------------------- */

  /* rdds ------------------------------------------------------------------ */


  /* dstreams -------------------------------------------------------------- */


  /* ----------------------------------------------------------------------- */
  /* delete from cassandra --------------------------------------------------*/
  /* ----------------------------------------------------------------------- */

  /* rdds ------------------------------------------------------------------ */


  /* dstreams ------------------------------------------------------------------ */



  /* ----------------------------------------------------------------------- */
  /* utilities for moving rdds and dstreams from and to pyspark ------------ */
  /* ----------------------------------------------------------------------- */


  def javaRDD(rdd: RDD[_]) = JavaRDD.fromRDD(rdd)

  def javaDStream(dstream: DStream[_]) = JavaDStream.fromDStream(dstream)

  def printOptions(): PrintOptions = {
    return PrintOptions.out
  }
}
