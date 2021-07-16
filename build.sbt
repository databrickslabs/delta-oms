import sbt.Level

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

name := "delta-oms"

organization := "com.databricks.labs"

scalaVersion := "2.12.10"

val gitUrl = "https://github.com/databrickslabs/delta-oms"
val sparkVersion = "3.1.1"
val deltaVersion = "1.0.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "com.github.pureconfig" %% "pureconfig" % "0.14.0",
  "io.delta" %% "delta-core" % deltaVersion % "provided",

  // Test Dependencies
  "org.scalatest" %% "scalatest" % "3.1.0" % Test,
  "junit" % "junit" % "4.12" % Test,
  "com.novocode" % "junit-interface" % "0.11" % Test,
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % Test classifier "tests",
  "org.apache.spark" %% "spark-core" % sparkVersion % Test classifier "tests",
  "org.apache.spark" %% "spark-sql" % sparkVersion % Test classifier "tests",
  "org.apache.spark" %% "spark-hive" % sparkVersion % Test classifier "tests"
)

parallelExecution in ThisBuild := false

crossScalaVersions in ThisBuild := Seq("2.12.10", "2.11.12")

javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

scalacOptions ++= Seq(
  "-target:jvm-1.8"
)

javaOptions += "-Xmx1024m"

parallelExecution in Test := false

fork in Test := true

javaOptions in Test ++= Seq(
  "-Dspark.ui.enabled=false",
  "-Dspark.ui.showConsoleProgress=false",
  "-Dspark.databricks.delta.snapshotPartitions=2",
  "-Dspark.sql.shuffle.partitions=5",
  "-Ddelta.log.cacheSize=3",
  "-Dspark.sql.sources.parallelPartitionDiscovery.parallelism=5",
  "-DOMS_CONFIG_FILE=inbuilt",
  "-Xmx1024m"
)

testOptions in Test += Tests.Argument("-oDF")

testOptions in Test += Tests.Argument(TestFrameworks.JUnit, "-v", "-a")

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

homepage := Some(url(gitUrl))
scmInfo := Some(ScmInfo(url(gitUrl), "git@github.com:databrickslabs/delta-oms.git"))
developers := List(Developer("himanishk", "Himanish Kushary", "himanish@databricks.com",
  url("https://github.com/himanishk")))
licenses += ("Databricks", url(gitUrl +"/blob/dev/LICENSE"))
publishMavenStyle := true

publishTo := Some(
  if (version.value.endsWith("SNAPSHOT")) {
    Opts.resolver.sonatypeSnapshots
  } else {
    Opts.resolver.sonatypeStaging
  }
)
