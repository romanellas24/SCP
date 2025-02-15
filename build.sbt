ThisBuild / version := "0.1.0-SNAPSHOT"

// Scala 2.12.18
ThisBuild / scalaVersion := "2.12.18"


// Spark 3.5.4
// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.4"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.4"

lazy val root = (project in file("."))
  .settings(
    name := "SCP",
    assembly / mainClass := Some("project.Second"),
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF","services",xs @ _*) => MergeStrategy.filterDistinctLines // Added this
      case PathList("META-INF",xs @ _*) => MergeStrategy.discard
      case _ => MergeStrategy.first
    }
  )

lazy val app = (project in file("app"))
  .settings(
    assembly / mainClass := Some("project.Second"),
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF","services",xs @ _*) => MergeStrategy.filterDistinctLines // Added this
      case PathList("META-INF",xs @ _*) => MergeStrategy.discard
      case _ => MergeStrategy.first
    }
  )

lazy val utils = (project in file("utils"))
  .settings(
    assembly / assemblyJarName := "utils.jar",
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF","services",xs @ _*) => MergeStrategy.filterDistinctLines // Added this
      case PathList("META-INF",xs @ _*) => MergeStrategy.discard
      case _ => MergeStrategy.first
    }
  )
