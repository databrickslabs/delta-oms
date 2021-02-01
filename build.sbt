name := "deltaods"

version := "0.1"

scalaVersion := "2.12.12"

val sparkVersion = "3.0.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-catalyst" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "com.github.pureconfig" %% "pureconfig" % "0.14.0",
  "io.delta" %% "delta-core" % "0.7.0"
)

parallelExecution in Test := false

fork in Test := true