name := "deltaods"

version := "0.1"

scalaVersion := "2.12.12"

val sparkVersion = "3.0.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "com.github.pureconfig" %% "pureconfig" % "0.14.0",
  "io.delta" %% "delta-core" % "0.7.0" % "provided"
)

parallelExecution in Test := false

fork in Test := true

test in assembly := {}

run in Compile := Defaults.runTask(fullClasspath in Compile,
  mainClass in (Compile, run), runner in (Compile, run)).evaluated

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("org.apache.spark.sql.delta.**" ->
    "com.databricks.sql.transaction.tahoe.@1").inAll
)

logLevel in assembly := Level.Error

