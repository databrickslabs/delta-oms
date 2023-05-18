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

import com.databricks.labs.deltaoms.common.Utils._
import com.databricks.labs.deltaoms.configuration.{ConfigurationSettings, EnvironmentResolver, OMSConfig}
import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.delta.test.DeltaTestSharedSession
import org.apache.spark.sql.test.SharedSparkSession

class OMSInitializerSuite extends QueryTest with SharedSparkSession with DeltaTestSharedSession
  with BeforeAndAfter with ConfigurationSettings with OMSInitializer {
  // scalastyle:on funsuite

  test("Validate empty Configuration Settings") {
    System.setProperty("OMS_ENV", "empty")
    assert(environmentType == "empty")
    assert(environment == EnvironmentResolver.fetchEnvironment("empty"))
    assert(omsConfig.schemaName.isEmpty)
    assert(omsConfig.catalogName.isEmpty)
    System.clearProperty("OMS_ENV")
  }

  test("Inbuilt Configuration Settings") {
    System.setProperty("OMS_ENV", "inbuilt")
    assert(environmentType == "inbuilt")
    assert(environment == EnvironmentResolver.fetchEnvironment("inbuilt"))
    assert(omsConfig.schemaName.get == "oms_default_inbuilt")
    assert(omsConfig.catalogName.isEmpty)
    assert(omsConfig.locationName.get == "inbuilt_location")
  }

  test("Initialize OMS Database and tables") {
    val dbName = omsConfig.schemaName.get
    assert(!spark.catalog.databaseExists(dbName))
    initializeOMS(omsConfig)
    assert(spark.catalog.databaseExists(dbName))
    // spark.sql(s"show tables in $dbName").show()
    // spark.sql(s"describe extended $dbName.${omsConfig.sourceConfigTable}").show(false)
    initializeOMS(omsConfig, dropAndRecreate = true)
    assert(spark.catalog.databaseExists(dbName))
    assert(spark.catalog.tableExists(dbName, omsConfig.sourceConfigTable))
    assert(spark.catalog.tableExists(dbName, omsConfig.pathConfigTable))
    assert(spark.catalog.tableExists(dbName, omsConfig.rawActionTable))
    assert(spark.catalog.tableExists(dbName, omsConfig.processedHistoryTable))
    assert(spark.catalog.tableExists(dbName, omsConfig.commitInfoSnapshotTable))
    assert(spark.catalog.tableExists(dbName, omsConfig.actionSnapshotTable))

    val sourceConfigSchema = spark.read.table(getSourceConfigTableName(omsConfig)).schema
    assert(sourceConfigSchema.equals(Schemas.sourceConfig))
    assert(sourceConfigSchema == Schemas.sourceConfig)

    val pathConfigSchema = spark.read.table(getPathConfigTableName(omsConfig)).schema
    assert(pathConfigSchema.equals(Schemas.pathConfig))
    assert(pathConfigSchema == Schemas.pathConfig)

    val rawActionsSchema =
      spark.read.format("delta").load(getRawActionsTableUrl(omsConfig)).schema
    assert(rawActionsSchema.fieldNames.sameElements(Schemas.rawAction.fieldNames))

    val processedHistorySchema = spark.read.table(getProcessedHistoryTableName(omsConfig)).schema
    assert(processedHistorySchema.equals(Schemas.processedHistory))
    assert(processedHistorySchema == Schemas.processedHistory)

    val commitSnapshotsSchema = spark.read.table(getCommitSnapshotsTableName(omsConfig)).schema
    assert(commitSnapshotsSchema.fieldNames.sameElements(Schemas.commitSnapshot.fieldNames))

    val actionSnapshotsSchema = spark.read.table(getActionSnapshotsTableName(omsConfig)).schema
    assert(actionSnapshotsSchema.fieldNames.sameElements(Schemas.actionSnapshot.fieldNames))
  }

  test("cleanupOMS DB Path Exception") {
    val dbInvalidPathOMSConfig =
      OMSConfig(schemaName = Some("abc"), locationUrl = Some("s3://sampleBase"))
    assert(intercept[java.lang.RuntimeException] {
      cleanupOMS(dbInvalidPathOMSConfig)
    }.getMessage.contains("org.apache.hadoop.fs.UnsupportedFileSystemException"))
  }
}
