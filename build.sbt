/*
 * Copyright (2021) Databricks, Inc.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR
 * IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 * See the Full License for the specific language governing permissions and
 * limitations under the License.
 */

val gitUrl = "https://github.com/databrickslabs/delta-oms"

inThisBuild(List(
  organization := "com.databricks.labs",
  homepage := Some(url(gitUrl)),
  licenses := List("Databricks" -> url(gitUrl +"/blob/master/LICENSE")),
  developers := List(
    Developer(
      "himanishk",
      "Himanish Kushary",
      "himanish@databricks.com",
      url("https://databricks.com")
    )
  )
))

Global / lintUnusedKeysOnLoad := false

ThisBuild / parallelExecution  := false
ThisBuild / scalastyleConfig   := baseDirectory.value / "scalastyle-config.xml"
// crossScalaVersions in ThisBuild := Seq("2.12.10", "2.11.12")

lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")
lazy val testScalastyle = taskKey[Unit]("testScalastyle")

val sparkVersion = "3.1.1"
val deltaVersion = "1.0.0"

lazy val commonSettings = Seq(
  name := "delta-oms",
  organization := "com.databricks.labs",
  scalaVersion := "2.12.10",
  javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
  scalacOptions ++= Seq("-target:jvm-1.8"),
  javaOptions += "-Xmx1024m",
  Test / parallelExecution := false,
  Test / fork := true,
  Test / javaOptions ++= Seq(
  "-Dspark.ui.enabled=false",
  "-Dspark.ui.showConsoleProgress=false",
  "-Dspark.databricks.delta.snapshotPartitions=2",
  "-Dspark.sql.shuffle.partitions=5",
  "-Ddelta.log.cacheSize=3",
  "-Dspark.sql.sources.parallelPartitionDiscovery.parallelism=5",
  "-DOMS_CONFIG_FILE=inbuilt",
  "-Xmx1024m"),
  Test / testOptions += Tests.Argument("-oDF"),
  Test / testOptions += Tests.Argument(TestFrameworks.JUnit, "-v", "-a"),
  scalastyleConfig := baseDirectory.value / "scalastyle-config.xml",
  compileScalastyle := (Compile / scalastyle).toTask("").value,
  Compile / compile := ((Compile / compile) dependsOn compileScalastyle).value,
  testScalastyle := (Test / scalastyle).toTask("").value,
  Test / test := ((Test / test) dependsOn testScalastyle).value,
  Compile / run := Defaults.runTask(Compile / fullClasspath,
    Compile / run / mainClass,
    Compile / run / runner).evaluated,

  coverageExcludedPackages :=
    """<empty>;com.databricks.labs.deltaoms.process.*;com.databricks.labs.deltaoms.init.*;
      |com.databricks.labs.deltaoms.ingest.*;
      |com.databricks.labs.deltaoms.model.*;
      |com.databricks.labs.deltaoms.common.*Runner;
      |com.databricks.labs.deltaoms.common.OMSStreamingQueryListener;
      |com.databricks.labs.deltaoms.common.Schemas;
      |com.databricks.labs.deltaoms.configuration.SparkSettings""".stripMargin
)

lazy val root = (project in file(".")).
  enablePlugins(AssemblyPlugin, BuildInfoPlugin).
  settings(commonSettings,
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-catalyst" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
      "com.github.pureconfig" %% "pureconfig" % "0.14.0" % "provided",
      "com.databricks" % "dbutils-api_2.12" % "0.0.5" % "provided",
      "io.delta" %% "delta-core" % deltaVersion % "provided",

      // Test Dependencies
      "org.scalatest" %% "scalatest" % "3.1.0" % Test,
      "junit" % "junit" % "4.12" % Test,
      "com.novocode" % "junit-interface" % "0.11" % Test,
      "org.apache.spark" %% "spark-catalyst" % sparkVersion % Test classifier "tests",
      "org.apache.spark" %% "spark-core" % sparkVersion % Test classifier "tests",
      "org.apache.spark" %% "spark-sql" % sparkVersion % Test classifier "tests",
      "org.apache.spark" %% "spark-hive" % sparkVersion % Test classifier "tests"
    ),
    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
    buildInfoPackage := "com.databricks.labs.deltaoms.common",
    assembly / test  := {},
    assembly / assemblyOption := (assembly / assemblyOption).value.copy(includeScala = false),
    assembly / assemblyShadeRules := Seq(
      ShadeRule.rename("org.apache.spark.sql.delta.**" ->
        "com.databricks.sql.transaction.tahoe.@1").inAll
    ),
    assembly / logLevel := Level.Error,
    assembly / artifact := {
      val art = (assembly / artifact).value
      art.withClassifier(None)
    },
    publish / skip := true
  )

lazy val distribution = project
  .settings(commonSettings,
    addArtifact(assembly / artifact, assembly),
    Compile / packageBin := ((root / Compile) / assembly).value,
  )

