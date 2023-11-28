ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "SparkWithCassandra - RDDs",
    libraryDependencies += "com.typesafe.play" %% "play-json" % "2.9.4",
    libraryDependencies += "com.datastax.oss" % "java-driver-core" % "4.15.0",
    libraryDependencies += "org.scala-lang.modules" %% "scala-collection-compat" % "2.11.0",
    libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "3.4.1",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.0",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.0",
  )
