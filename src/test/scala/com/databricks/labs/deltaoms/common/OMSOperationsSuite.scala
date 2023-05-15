/*
 * Copyright (2021) Databricks, Inc.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE
 * AND NONINFRINGEMENT.
 *
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR
 * THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 * See the Full License for the specific language governing permissions and
 * limitations under the License.
 */

package com.databricks.labs.deltaoms.common

import java.util.UUID

import com.databricks.labs.deltaoms.configuration.ConfigurationSettings
import com.databricks.labs.deltaoms.mock.MockDeltaTransactionGenerator
import Utils._

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.delta.test.DeltaTestSharedSession
import org.apache.spark.sql.test.SharedSparkSession
import com.databricks.labs.deltaoms.common.OMSOperations._
import com.databricks.labs.deltaoms.common.Utils.getPathConfigTableName
import com.databricks.labs.deltaoms.model.SourceConfig
import com.databricks.labs.deltaoms.utils.UtilityOperations
import com.databricks.labs.deltaoms.utils.UtilityOperations.listSubDirectories

import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.util.SerializableConfiguration

class OMSOperationsSuite extends QueryTest
  with SharedSparkSession
  with DeltaTestSharedSession
  with ConfigurationSettings
  with OMSInitializer
  with StreamTest
  with MockDeltaTransactionGenerator {

  import testImplicits._

  val baseMockDir: String = "/tmp/spark-warehouse/mock";
  val uniqTableId: String = UUID.randomUUID.toString.replace("-", "").take(7)
  val db1Name = s"omstestingdb_${uniqTableId}_1"
  val db2Name = s"omstestingdb_${uniqTableId}_2"
  val table1Name = "test_table_1"
  val table2Name = "test_table_2"
  val table3Name = "test_table_3"

  val mockTables: Seq[MockTableInfo] =
    Seq(MockTableInfo(MockDBInfo(db1Name, baseMockDir), table1Name),
      MockTableInfo(MockDBInfo(db1Name, baseMockDir), table3Name),
      MockTableInfo(MockDBInfo(db2Name, baseMockDir), table2Name))

  override def beforeAll(): Unit = {
    super.beforeAll()
    initializeOMS(omsConfig, dropAndRecreate = true)
    createMockDatabaseAndTables(mockTables)
    // spark.sql(s"show tables in $db1Name").show()
    // spark.sql(s"describe extended $db1Name.$table1Name").show(false)
    spark.sql(s"INSERT INTO ${omsConfig.schemaName.get}.${omsConfig.sourceConfigTable} " +
      s"VALUES('$baseMockDir/**', false)")
    updateOMSPathConfigFromSourceConfig(omsConfig)
  }

  override def afterAll(): Unit = {
    try {
      cleanupDatabases(mockTables)
      cleanupOMS(omsConfig)
    } finally {
      super.afterAll()
    }
  }

  test("Mock Database and tables initialized") {
    assert(spark.catalog.databaseExists(db1Name))
    assert(spark.catalog.tableExists(db1Name, table1Name))
    assert(spark.catalog.tableExists(db1Name, table3Name))
    assert(spark.catalog.tableExists(db2Name, table2Name))
  }

  test("OMS Source Configured") {
    val sourcePaths = spark.sql(
      s"SELECT path FROM ${omsConfig.schemaName.get}.${omsConfig.sourceConfigTable}")
    checkDatasetUnorderly(sourcePaths.as[String], s"$baseMockDir/**")
  }

  test("fetchSourceConfigForProcessing") {
    val srcConfigs = fetchSourceConfigForProcessing(omsConfig)
    assert(srcConfigs.nonEmpty)
    val paths = srcConfigs.map(_.path)
    assert(Array(s"file:$baseMockDir/$db1Name/$table1Name",
        s"file:$baseMockDir/$db1Name/$table3Name",
        s"file:$baseMockDir/$db2Name/$table2Name").forall(paths.contains))
  }

  test("updateOMSPathConfigFromSourceConfig") {
    updateOMSPathConfigFromSourceConfig(omsConfig)
    val pathConfigTable = s"${omsConfig.schemaName.get}.${omsConfig.pathConfigTable}"
    checkAnswer(spark.sql(s"SELECT count(*) FROM $pathConfigTable"), Row(3))
    checkAnswer(spark.sql(s"SELECT count(distinct ${Utils.WUID}) FROM $pathConfigTable"), Row(2))
    checkAnswer(spark.sql(s"SELECT count(distinct ${Utils.PUID}) FROM $pathConfigTable"), Row(3))
    val qns = spark.sql(s"SELECT qualifiedName FROM $pathConfigTable").as[String].collect()
    assert(qns.length == 3)
    assert(Array(s"file:$baseMockDir/$db1Name/$table1Name",
      s"file:$baseMockDir/$db1Name/$table3Name",
      s"file:$baseMockDir/$db2Name/$table2Name").forall(qns.contains))
  }

  test("updateOMSPathConfigFromList") {
    val configuredSources: Array[SourceConfig] = fetchSourceConfigForProcessing(omsConfig)
    updateOMSPathConfigFromList(configuredSources.toSeq,
      getPathConfigTableName(omsConfig), truncate = true)
    val pathConfigTable = s"${omsConfig.schemaName.get}.${omsConfig.pathConfigTable}"
    checkAnswer(spark.sql(s"SELECT count(*) FROM $pathConfigTable"), Row(3))
    checkAnswer(spark.sql(s"SELECT count(distinct ${Utils.WUID}) FROM $pathConfigTable"), Row(2))
    checkAnswer(spark.sql(s"SELECT count(distinct ${Utils.PUID}) FROM $pathConfigTable"), Row(3))
    val qns = spark.sql(s"SELECT qualifiedName FROM $pathConfigTable").as[String].collect()
    assert(qns.length == 3)
    assert(Array(s"file:$baseMockDir/$db1Name/$table1Name",
      s"file:$baseMockDir/$db1Name/$table3Name",
      s"file:$baseMockDir/$db2Name/$table2Name").forall(qns.contains))
  }

  test("Wildcardpath fetchPathForStreamProcessing") {
    val streamPaths = fetchPathForStreamProcessing(getPathConfigTableName(omsConfig))
    assert(streamPaths.nonEmpty)
    assert(streamPaths.map(_._1).length == 2)
    assert(streamPaths.map(_._1).exists(_ endsWith "_delta_log/*.json"))
  }

  test("Wildcardpath startingStream fetchPathForStreamProcessing") {
    val streamPaths = fetchPathForStreamProcessing(getPathConfigTableName(omsConfig),
      endingStream = 1)
    assert(streamPaths.nonEmpty)
    assert(streamPaths.map(_._1).length == 1)
    assert(streamPaths.map(_._1).exists(_ endsWith "_delta_log/*.json"))
  }

  test("Non Wildcardpath fetchPathForStreamProcessing") {
    val streamPaths = fetchPathForStreamProcessing(getPathConfigTableName(omsConfig),
      useWildCardPath = false)
    assert(streamPaths.nonEmpty)
    assert(streamPaths.map(_._1).length == 3)
    assert(streamPaths.map(_._1).exists(_ endsWith "_delta_log/*.json"))
  }

  test("streamingUpdateRawDeltaActionsToOMS") {
    streamingUpdateRawDeltaActionsToOMS(omsConfig.copy(useAutoloader = false))
    val rawActionsTable = getRawActionsTableName(omsConfig)
    checkAnswer(spark.sql(s"SELECT count(*) FROM $rawActionsTable"), Row(381))
  }

  test("getCurrentRawActionsVersion") {
    val maxVersion = getCurrentRawActionsVersion(getRawActionsTableName(omsConfig))
    assert(maxVersion == 2)
  }

  test("processCommitInfoFromRawActions") {
    val rawActions = spark.read.table(s"${getRawActionsTableName(omsConfig)}")
    processCommitInfoFromRawActions(rawActions,
      getCommitSnapshotTableName(omsConfig))
    checkAnswer(spark.sql(s"SELECT count(*) FROM ${getCommitSnapshotTableName(omsConfig)}"),
      Row(15))
  }

 test("processActionSnapshotsFromRawActions") {
    val rawActions = spark.read.table(s"${getRawActionsTableName(omsConfig)}")
    processActionSnapshotsFromRawActions(rawActions,
      getActionSnapshotTableName(omsConfig))
    checkAnswer(spark.sql(s"SELECT count(*) FROM ${getActionSnapshotTableName(omsConfig)}"),
      Row(1485))
  }

  test("updateLastProcessedRawActions and getLastProcessedRawActionsVersion") {
    updateLastProcessedRawActions(3L,
      omsConfig.rawActionTable,
      getProcessedHistoryTableName(omsConfig))
    checkAnswer(spark.sql(s"SELECT tableName, lastVersion FROM" +
      s" ${getProcessedHistoryTableName(omsConfig)}"),
      Row("raw_actions", 3))
    val lastVersion = getLastProcessedRawActionsVersion(getProcessedHistoryTableName(omsConfig),
      omsConfig.rawActionTable)
    assert(lastVersion == 3L)
  }

  test("recursiveListDeltaTablePaths") {
    val wildCardSourceConfig = SourceConfig("/tmp/spark-warehouse/")
    val hadoopConf = new SerializableConfiguration(spark.sessionState.newHadoopConf())

    val wildCardSubDirectories = listSubDirectories(wildCardSourceConfig, hadoopConf)
    assert(wildCardSubDirectories.length == 2)
    val wildCardTablePaths = wildCardSubDirectories
      .flatMap(UtilityOperations.recursiveListDeltaTablePaths(_, hadoopConf))
    assert(wildCardTablePaths.length == 9)
    assert(wildCardTablePaths.
      contains(SourceConfig("file:/tmp/spark-warehouse/oms.db/oms_default_inbuilt/raw_actions")))
  }

  test("resolveDeltaLocation") {
    val validateDB =
      UtilityOperations.resolveDeltaLocation(SourceConfig(s"hive_metastore.${db1Name}"))
    assert(validateDB.head._1 == s"`${db1Name}`.`${table1Name}`")
    assert(validateDB.head._2.get == s"file:${baseMockDir}/${db1Name}/${table1Name}")

    val validatedTable = UtilityOperations.resolveDeltaLocation(
      SourceConfig(s"hive_metastore.${db1Name}.${table1Name}"))
    assert(validatedTable.head._1 == s"`${db1Name}`.`${table1Name}`")
    assert(validatedTable.head._2.get == s"file:${baseMockDir}/${db1Name}/${table1Name}")

    val validatedPath =
      UtilityOperations.resolveDeltaLocation(SourceConfig(s"$baseMockDir/**"))
    assert(validatedPath.head._1 == s"$baseMockDir/**")
  }
}
