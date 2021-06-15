/*
 * Copyright (2020) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

name := "delta-oms"

organization := "io.delta"

scalaVersion := "2.12.10"

val sparkVersion = "3.1.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "com.github.pureconfig" %% "pureconfig" % "0.14.0",
  "io.delta" %% "delta-core" % "1.0.0" % "provided",

  // Test Dependencies
  "org.scalatest" %% "scalatest" % "3.1.0" % Test,
  "junit" % "junit" % "4.12" % Test,
  "com.novocode" % "junit-interface" % "0.11" % Test,
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % Test classifier "tests",
  "org.apache.spark" %% "spark-core" % sparkVersion % Test classifier "tests",
  "org.apache.spark" %% "spark-sql" % sparkVersion % Test classifier "tests",
  "org.apache.spark" %% "spark-hive" % sparkVersion % Test classifier "tests",
  // Compiler plugins
  // -- Bump up the genjavadoc version explicitly to 0.16 to work with Scala 2.12
  compilerPlugin("com.typesafe.genjavadoc" %% "genjavadoc-plugin" % "0.16" cross CrossVersion.full)
)

testOptions in Test += Tests.Argument("-oDF")

testOptions in Test += Tests.Argument(TestFrameworks.JUnit, "-v", "-a")

parallelExecution in Test := false

fork in Test := true

scalacOptions ++= Seq(
  "-target:jvm-1.8",
  "-P:genjavadoc:strictVisibility=true" // hide package private types and methods in javadoc
)

javaOptions += "-Xmx1024m"

javaOptions in Test ++= Seq(
  "-Dspark.ui.enabled=false",
  "-Dspark.ui.showConsoleProgress=false",
  "-Dspark.databricks.delta.snapshotPartitions=2",
  "-Dspark.sql.shuffle.partitions=5",
  "-Ddelta.log.cacheSize=3",
  "-Dspark.sql.sources.parallelPartitionDiscovery.parallelism=5",
  "-Xmx1024m"
)

/**********************
 * ScalaStyle settings *
 **********************/

scalastyleConfig := baseDirectory.value / "scalastyle-config.xml"

lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")

compileScalastyle := scalastyle.in(Compile).toTask("").value

(compile in Compile) := ((compile in Compile) dependsOn compileScalastyle).value

lazy val testScalastyle = taskKey[Unit]("testScalastyle")

testScalastyle := scalastyle.in(Test).toTask("").value

(test in Test) := ((test in Test) dependsOn testScalastyle).value

test in assembly := {}

run in Compile := Defaults.runTask(fullClasspath in Compile,
  mainClass in(Compile, run), runner in(Compile, run)).evaluated

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("org.apache.spark.sql.delta.**" ->
    "com.databricks.sql.transaction.tahoe.@1").inAll
)

logLevel in assembly := Level.Error


