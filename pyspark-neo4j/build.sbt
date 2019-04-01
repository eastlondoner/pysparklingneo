import scala.io

name := "pyspark-neo4j"

version := io.Source.fromFile("version.txt").mkString.trim

organization := "eastlondoner"

scalaVersion := "2.12.8"

credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")

licenses += "Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0")

libraryDependencies ++= Seq(
  "net.razorvine" % "pyrolite" % "4.20",
  "org.opencypher" % "spark-cypher" % "0.3.1"
)

spName := "eastlondoner/pyspark-neo4j"

sparkVersion := "2.4.0"

sparkComponents ++= Seq("core", "streaming", "sql")
javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

assemblyOption in assembly := (assemblyOption in assembly).value.copy(
  includeScala = false
)

spIgnoreProvided := true

ivyScala := ivyScala.value map {
  _.copy(overrideScalaVersion = true)
}

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case PathList("META-INF", xs@_*) => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
