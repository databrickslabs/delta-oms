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
import com.databricks.labs.deltaoms.common.Utils.getPathConfigTablePath
import com.databricks.labs.deltaoms.model.SourceConfig
import com.databricks.labs.deltaoms.utils.UtilityOperations
import com.databricks.labs.deltaoms.utils.UtilityOperations.listSubDirectories

import org.apache.spark.sql.catalyst.TableIdentifier
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

  val baseMockDir: String = "/tmp/spark-warehouse";
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
    spark.sql(s"INSERT INTO ${omsConfig.dbName.get}.${omsConfig.sourceConfigTable} " +
      s"VALUES('$db1Name', false, Map('wildCardLevel','0'))")
    spark.sql(s"INSERT INTO ${omsConfig.dbName.get}.${omsConfig.sourceConfigTable} " +
      s"VALUES('$db2Name', false, Map('wildCardLevel','0'))")
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
      s"SELECT path FROM ${omsConfig.dbName.get}.${omsConfig.sourceConfigTable}")
    checkDatasetUnorderly(sourcePaths.as[String], db1Name, db2Name)
  }

  test("fetchSourceConfigForProcessing") {
    val srcConfigs = fetchSourceConfigForProcessing(omsConfig)
    assert(srcConfigs.nonEmpty)
    assert(srcConfigs.map(_.path).sorted.sameElements(Seq(db1Name, db2Name).sorted))
  }

  test("updateOMSPathConfigFromSourceConfig") {
    updateOMSPathConfigFromSourceConfig(omsConfig)
    val pathConfigTable = s"${omsConfig.dbName.get}.${omsConfig.pathConfigTable}"
    checkAnswer(spark.sql(s"SELECT count(*) FROM $pathConfigTable"), Row(3))
    checkAnswer(spark.sql(s"SELECT count(distinct ${Utils.WUID}) FROM $pathConfigTable"), Row(2))
    checkAnswer(spark.sql(s"SELECT count(distinct ${Utils.PUID}) FROM $pathConfigTable"), Row(3))
    checkDatasetUnorderly(spark.sql(s"SELECT qualifiedName FROM $pathConfigTable").as[String],
      s"${db1Name}.${table1Name}", s"${db1Name}.${table3Name}",
      s"${db2Name}.${table2Name}")
  }

  test("updateOMSPathConfigFromList") {
    val configuredSources: Array[SourceConfig] = fetchSourceConfigForProcessing(omsConfig)
    updateOMSPathConfigFromList(configuredSources.toSeq,
      getPathConfigTablePath(omsConfig), truncate = true)
    val pathConfigTable = s"${omsConfig.dbName.get}.${omsConfig.pathConfigTable}"
    checkAnswer(spark.sql(s"SELECT count(*) FROM $pathConfigTable"), Row(3))
    checkAnswer(spark.sql(s"SELECT count(distinct ${Utils.WUID}) FROM $pathConfigTable"), Row(2))
    checkAnswer(spark.sql(s"SELECT count(distinct ${Utils.PUID}) FROM $pathConfigTable"), Row(3))
    checkDatasetUnorderly(spark.sql(s"SELECT qualifiedName FROM $pathConfigTable").as[String],
      s"${db1Name}.${table1Name}", s"${db1Name}.${table3Name}",
      s"${db2Name}.${table2Name}")
  }

  test("Wildcardpath fetchPathForStreamProcessing") {
    val streamPaths = fetchPathForStreamProcessing(getPathConfigTablePath(omsConfig))
    assert(streamPaths.nonEmpty)
    assert(streamPaths.map(_._1).length == 2)
    assert(streamPaths.map(_._1).exists(_ endsWith "_delta_log/*.json"))
  }

  test("Wildcardpath startingStream fetchPathForStreamProcessing") {
    val streamPaths = fetchPathForStreamProcessing(getPathConfigTablePath(omsConfig),
      endingStream = 1)
    assert(streamPaths.nonEmpty)
    assert(streamPaths.map(_._1).length == 1)
    assert(streamPaths.map(_._1).exists(_ endsWith "_delta_log/*.json"))
  }

  test("Non Wildcardpath fetchPathForStreamProcessing") {
    val streamPaths = fetchPathForStreamProcessing(getPathConfigTablePath(omsConfig),
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
    val maxVersion = getCurrentRawActionsVersion(getRawActionsTablePath(omsConfig))
    assert(maxVersion == 2)
  }

  test("processCommitInfoFromRawActions") {
    val rawActions = spark.read.format("delta")
      .load(s"${getRawActionsTablePath(omsConfig)}")
    processCommitInfoFromRawActions(rawActions,
      getCommitSnapshotTablePath(omsConfig),
      getCommitSnapshotTableName(omsConfig))
    checkAnswer(spark.sql(s"SELECT count(*) FROM ${getCommitSnapshotTableName(omsConfig)}"),
      Row(15))
  }

 test("processActionSnapshotsFromRawActions") {
    val rawActions = spark.read.format("delta")
      .load(s"${getRawActionsTablePath(omsConfig)}")
    processActionSnapshotsFromRawActions(rawActions,
      getActionSnapshotTablePath(omsConfig),
      getActionSnapshotTableName(omsConfig))
    checkAnswer(spark.sql(s"SELECT count(*) FROM ${getActionSnapshotTableName(omsConfig)}"),
      Row(1485))
  }

  test("updateLastProcessedRawActions and getLastProcessedRawActionsVersion") {
    updateLastProcessedRawActions(3L,
      omsConfig.rawActionTable,
      getProcessedHistoryTablePath(omsConfig))
    checkAnswer(spark.sql(s"SELECT tableName, lastVersion FROM" +
      s" delta.`${getProcessedHistoryTablePath(omsConfig)}`"),
      Row("raw_actions", 3))
    val lastVersion = getLastProcessedRawActionsVersion(getProcessedHistoryTablePath(omsConfig),
      omsConfig.rawActionTable)
    assert(lastVersion == 3L)
  }

  test("validateDeltaLocation") {
    val validatedTable =
      UtilityOperations.validateDeltaLocation(SourceConfig(s"${db1Name}.${table1Name}"))
    assert(validatedTable.head._1.get == s"${db1Name}.${table1Name}")
    assert(validatedTable.head._2 == s"file:${baseMockDir}/${db1Name}/${table1Name}")

    val pathValidatedTable =
      UtilityOperations.validateDeltaLocation(SourceConfig(getProcessedHistoryTablePath(omsConfig)))
    assert(pathValidatedTable.head._1.isEmpty)
    assert(pathValidatedTable.head._2 == getProcessedHistoryTablePath(omsConfig))

    assertThrows[org.apache.spark.sql.AnalysisException](UtilityOperations
      .validateDeltaLocation(SourceConfig("Invalid")))
  }

  test("updateOMSPathConfigFromMetaStore") {
    val pathConfigTable = s"${omsConfig.dbName.get}.${omsConfig.pathConfigTable}"
    checkAnswer(spark.sql(s"SELECT count(*) FROM $pathConfigTable"), Row(3))
    updateOMSPathConfigFromMetaStore(omsConfig)
    checkAnswer(spark.sql(s"SELECT count(*) FROM $pathConfigTable"), Row(12))
  }

  test("fetchMetaStoreDeltaTables") {
    val tableId = UtilityOperations.fetchMetaStoreDeltaTables(Some(db1Name), Some("*table_1"))
    assert(tableId.length == 1)
    assert(tableId.head.identifier == s"${table1Name}")
    val tableIds = UtilityOperations.fetchMetaStoreDeltaTables(Some(db1Name), Some("*"))
    assert(tableIds.length == 2)
  }

  test("tableIdentifierToDeltaTableIdentifier") {
    assert(UtilityOperations.
      tableIdentifierToDeltaTableIdentifier(TableIdentifier("NonExistingTable")).isEmpty)
  }

  test("recursiveListDeltaTablePaths") {
    val wildCardSourceConfig = SourceConfig("/tmp/spark-warehouse/")
    val hadoopConf = new SerializableConfiguration(spark.sessionState.newHadoopConf())

    val wildCardSubDirectories = listSubDirectories(wildCardSourceConfig, hadoopConf)
    assert(wildCardSubDirectories.length == 5)
    val wildCardTablePaths = wildCardSubDirectories
      .flatMap(UtilityOperations.recursiveListDeltaTablePaths(_, hadoopConf))
    assert(wildCardTablePaths.length == 9)
    assert(wildCardTablePaths.
      contains(SourceConfig("file:/tmp/spark-warehouse/oms.db/oms_default_inbuilt/raw_actions")))
  }

}
