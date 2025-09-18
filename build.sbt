name := "finalproject"

version := "0.1"

scalaVersion := "2.12.19"

// apache Spark dependencies for data processing
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.4.0",
  "org.apache.spark" %% "spark-sql" % "3.4.0",
  "org.scalatest" %% "scalatest" % "3.2.15" % Test
)

// compiler options for better functional programming support
scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked",
  "-Xlint"
)