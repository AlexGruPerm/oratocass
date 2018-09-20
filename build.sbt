name := "oratocass"
version := "0.1"
scalaVersion := "2.11.0"
version := "1.0-SNAPSHOT"




libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"

libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-core" % "3.5.0"

scalaSource in Compile := baseDirectory.value / "src"
unmanagedBase := baseDirectory.value / "lib"

