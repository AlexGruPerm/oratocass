name := "oratocass"
version := "0.1"
scalaVersion := "2.11.0"
version := "1.0-SNAPSHOT"

libraryDependencies += "org.scala-lang" % "scala-library" % "2.11.0"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.1"
//libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.3.1"
libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-core" % "3.5.0"

//dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.9.6" force()
//dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.6" force()
//libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.3.1" % "provided"

dependencyOverrides ++= Set(
  "com.fasterxml.jackson.core" % "jackson-core" % "2.9.6",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.6"
)