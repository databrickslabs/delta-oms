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

import com.databricks.labs.deltaoms.common.OMSOperations._
import com.databricks.labs.deltaoms.common.Utils._
import com.databricks.labs.deltaoms.configuration.{ConfigurationSettings, OMSConfig}
import com.databricks.labs.deltaoms.mock.MockDeltaTransactionGenerator
import com.databricks.labs.deltaoms.model.SourceConfig

import org.apache.spark.sql.{Dataset, QueryTest, Row}
import org.apache.spark.sql.delta.test.DeltaTestSharedSession
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.test.SharedSparkSession

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
      s"VALUES('$baseMockDir/$db1Name', false)")
    spark.sql(s"INSERT INTO ${omsConfig.schemaName.get}.${omsConfig.sourceConfigTable} " +
      s"VALUES('$baseMockDir/$db2Name', false)")
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
    checkDatasetUnorderly(sourcePaths.as[String],
    s"$baseMockDir/$db1Name", s"$baseMockDir/$db2Name")
  }

  test("fetchSourceConfigForProcessing") {
    val srcConfigs = fetchSourceConfigForProcessing(getSourceConfigTableUrl(omsConfig))
      .select("path")
    checkDatasetUnorderly(srcConfigs.as[String],
    s"$baseMockDir/$db1Name",
    s"$baseMockDir/$db2Name")
  }

  test("Wildcardpath fetchPathForStreamProcessing") {
    val streamPaths = fetchPathForStreamProcessing(getSourceConfigTableUrl(omsConfig))
    assert(streamPaths.nonEmpty)
    assert(streamPaths.map(_._1).length == 2)
    assert(streamPaths.map(_._1).exists(_ endsWith "_delta_log/*.json"))
  }

  test("Wildcardpath startingStream fetchPathForStreamProcessing") {
    val streamPaths = fetchPathForStreamProcessing(getSourceConfigTableUrl(omsConfig),
      endingStream = 1)
    assert(streamPaths.nonEmpty)
    assert(streamPaths.map(_._1).length == 1)
    assert(streamPaths.map(_._1).exists(_ endsWith "_delta_log/*.json"))
  }

  test("streamingUpdateRawDeltaActionsToOMS Consolidated WildCardPaths") {
    streamingUpdateRawDeltaActionsToOMS(omsConfig.copy(useAutoloader = false))
    val rawActionsTable = getRawActionsTableName(omsConfig)
    checkAnswer(spark.sql(s"SELECT count(*) FROM $rawActionsTable"), Row(381))
  }

  test("streamingUpdateRawDeltaActionsToOMS Non Consolidated WildCardPaths") {
    streamingUpdateRawDeltaActionsToOMS(
      omsConfig.copy(consolidateWildcardPaths = false, useAutoloader = false))
    val rawActionsTable = getRawActionsTableName(omsConfig)
    checkAnswer(spark.sql(s"SELECT count(*) FROM $rawActionsTable"), Row(381))
  }

  test("getCurrentRawActionsVersion") {
    val maxVersion = getCurrentRawActionsVersion(getRawActionsTableUrl(omsConfig))
    assert(maxVersion == 2)
  }

  test("processCommitInfoFromRawActions") {
    val rawActions = spark.read.format("delta")
      .load(s"${getRawActionsTableUrl(omsConfig)}")
    processCommitInfoFromRawActions(rawActions,
      getCommitSnapshotsTableUrl(omsConfig))
    checkAnswer(spark.sql(s"SELECT count(*) FROM ${getCommitSnapshotsTableName(omsConfig)}"),
      Row(15))
  }

  test("processActionSnapshotsFromRawActions Initial") {
    val rawActions = spark.read.format("delta")
      .load(s"${getRawActionsTableUrl(omsConfig)}")
    processActionSnapshotsFromRawActions(rawActions,
      getActionSnapshotsTableUrl(omsConfig))
    checkAnswer(spark.sql(s"SELECT count(*) FROM ${getActionSnapshotsTableName(omsConfig)}"),
      Row(1485))
  }

  test("processActionSnapshotsFromRawActions Next") {
    val rawActions = spark.read.format("delta")
      .load(s"${getRawActionsTableUrl(omsConfig)}")
    processActionSnapshotsFromRawActions(rawActions,
      getActionSnapshotsTableUrl(omsConfig))
    checkAnswer(spark.sql(s"SELECT count(*) FROM ${getActionSnapshotsTableName(omsConfig)}"),
      Row(1485))
  }

  test("updateLastProcessedRawActions and getLastProcessedRawActionsVersion") {
    updateLastProcessedRawActions(3L,
      omsConfig.rawActionTable,
      getProcessedHistoryTableUrl(omsConfig))
    checkAnswer(spark.sql(s"SELECT tableName, lastVersion FROM" +
      s" ${getProcessedHistoryTableName(omsConfig)}"),
      Row("raw_actions", 3))
    val lastVersion = getLastProcessedRawActionsVersion(getProcessedHistoryTableUrl(omsConfig),
      omsConfig.rawActionTable)
    assert(lastVersion == 3L)
  }

  test("getCurrentRawActionsVersion Invalid Table URL") {
    val exception = intercept[java.lang.RuntimeException](
      getCurrentRawActionsVersion(s"${getRawActionsTableUrl(omsConfig)}xtra"))
    assert(exception.getMessage.contains("Unable to access the " +
      "RawActions table for getting current RawActions version."))
  }

  test("updateLastProcessedRawActions Invalid Table URL") {
    val exception = intercept[java.lang.RuntimeException](
      updateLastProcessedRawActions(1L, "rawactions",
        s"${getProcessedHistoryTableUrl(omsConfig)}xtra"))
    assert(exception.getMessage.contains("Unable to update the " +
      "Processed History table."))
  }

  test("processCommitInfoFromRawActions Invalid Table URL") {
    val rawActions = spark.read.format("delta")
      .load(s"${getRawActionsTableUrl(omsConfig)}")
    val exception = intercept[java.lang.RuntimeException](
      processCommitInfoFromRawActions(rawActions,
        s"${getCommitSnapshotsTableUrl(omsConfig)}xtra"))
    assert(exception.getMessage.contains("Unable to update the Commit Info " +
      "Snapshot table."))
  }

  test("processActionSnapshotsFromRawActions Invalid Table URL") {
    val rawActions = spark.read.format("delta")
      .load(s"${getRawActionsTableUrl(omsConfig)}")
    val exception = intercept[java.lang.RuntimeException](
      processActionSnapshotsFromRawActions(rawActions,
        s"${getActionSnapshotsTableUrl(omsConfig)}xtra"))
    assert(exception.getMessage.contains("Unable to access the ActionSnapshot Table"))
  }

  test("getLatestRawActionsVersion commit version") {
    val rawActions = getUpdatedRawActions(0, getRawActionsTableUrl(omsConfig))
    assert(getLatestRawActionsVersion(rawActions) == 2)
  }
}
