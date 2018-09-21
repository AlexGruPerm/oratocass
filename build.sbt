name := "oratocass"
version := "0.1"
scalaVersion := "2.11.8"
version := "1.0"

val sparkVersion = "2.3.0"

lazy val versions = new {
  val jackson_module = "2.8.8"
}

resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-library" % "2.11.8",
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion, //, "com.datastax.cassandra" % "cassandra-driver-core" % "3.5.0",

  "com.fasterxml.jackson.module" % "jackson-module-parameter-names" % versions.jackson_module,
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % versions.jackson_module

)

unmanagedBase := baseDirectory.value / "lib"

