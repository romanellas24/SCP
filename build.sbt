ThisBuild / version := "0.1.0-SNAPSHOT"

// Scala 2.12.18
ThisBuild / scalaVersion := "2.12.18"


// Spark 3.5.4
// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.4"
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.4"


// Jdk 21.0.5


lazy val root = (project in file("."))
  .settings(
    name := "untitled"
  )
