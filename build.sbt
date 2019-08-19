name := "SchemaOnReadWithSpark"

version := "0.1"

scalaVersion := "2.12.0"
val sparkVersion = "2.4.3"

//---------------------
// Spark Framework
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion

//---------------------
// Testing Frameworks
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % "test"
