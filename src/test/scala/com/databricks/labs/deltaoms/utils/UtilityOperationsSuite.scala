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

package com.databricks.labs.deltaoms.utils

import com.databricks.labs.deltaoms.common.{OMSInitializer, Schemas}
import com.databricks.labs.deltaoms.common.Utils.{omsCatalogDefinition, omsExternalLocationDefinition, omsSchemaDefinition, pathConfigTableDefinition, processedHistoryTableDefinition, puidCommitDatePartitions, rawActionsTableDefinition, sourceConfigDefinition}
import com.databricks.labs.deltaoms.configuration.{ConfigurationSettings, OMSConfig}
import com.databricks.labs.deltaoms.model.SchemaDefinition
import com.databricks.labs.deltaoms.utils.UtilityOperations.{executeSQL, isUCEnabled, resolveWildCardPath}
import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.delta.test.DeltaTestSharedSession
import org.apache.spark.sql.test.SharedSparkSession

class UtilityOperationsSuite extends QueryTest with SharedSparkSession with DeltaTestSharedSession
  with BeforeAndAfter with ConfigurationSettings {
  import testImplicits._
  // scalastyle:on funsuite

  test("Consolidate WildcardPaths") {
    val wcPaths1 = Array(("file:/tmp/oms/*/*/_delta_log/*.json", "abcd"),
      ("file:/tmp/oms/test_db_jun16/*/_delta_log/*.json", "efgh"))
    assert(UtilityOperations.consolidateWildCardPaths(wcPaths1).size == 1)

    val wcPaths2 = Array(
      ("file:/home/user/oms/test_db_jun16/*/_delta_log/*.json", "efgh"),
      ("file:/home/user/oms/*/*/_delta_log/*.json", "abcd"))
    assert(UtilityOperations.consolidateWildCardPaths(wcPaths2).size == 1)

    val wcPaths3 = Array(
      ("dbfs:/databricks-datasets/*/*/*/*/_delta_log/*.json", "006d76f"),
      ("dbfs:/databricks-datasets/*/*/*/_delta_log/*.json", "006d76f"),
      ("dbfs:/databricks-datasets/*/*/_delta_log/*.json", "3a6538e"),
      ("dbfs:/databricks-datasets/*/_delta_log/*.json", "32cb366"))
    assert(UtilityOperations.consolidateWildCardPaths(wcPaths3).size == 1)
  }

  test("Invalid Wild Card Level") {
    assertThrows[java.lang.AssertionError](
      resolveWildCardPath("dbfs:/warehouse/deltaoms/table_6", 2))
    assertThrows[java.lang.AssertionError](
      resolveWildCardPath("dbfs:/warehouse/deltaoms/table_6", -2))
  }

  test("Valid Wild Card Level") {
    val basePath = "dbfs:/warehouse/deltaoms/table_6"
    assert(resolveWildCardPath(basePath, 1)
      == "dbfs:/warehouse/*/*/_delta_log/*.json", "WildCard Level = 1")
    assert(resolveWildCardPath(basePath, 0)
      == "dbfs:/warehouse/deltaoms/*/_delta_log/*.json", "WildCard Level = 0")
    assert(resolveWildCardPath(basePath, -1)
      == "dbfs:/warehouse/deltaoms/table_6/_delta_log/*.json", "WildCard Level = -1")
  }

  test("Error creating Database") {
    val nonWorkingDbQuery: (String, String) = UtilityOperations.
      schemaCreationQuery(SchemaDefinition("default", "NonWorkingDB", "default.`NonWorkingDB`",
        Some("//tmp/testdb/nonworking")))
    val exception = intercept[java.lang.RuntimeException](executeSQL
    (nonWorkingDbQuery._1, nonWorkingDbQuery._2))
    assert(exception.getMessage.contains("Unable to create DATABASE"))
  }

  test("Valid External Location Creation SQL") {
    val testConfig = OMSConfig(locationName = Some("deltaoms-external-location"),
      storageCredentialName = Some("field_demos_credential"),
      locationUrl = Some("/deltaoms/deltaoms"))
    val extLocQuery = UtilityOperations.externalLocationCreationQuery(
      omsExternalLocationDefinition(testConfig))
    assert(extLocQuery._1 ==
      """CREATE EXTERNAL LOCATION IF NOT EXISTS `deltaoms-external-location`
         |URL '/deltaoms/deltaoms'
         |WITH (STORAGE CREDENTIAL `field_demos_credential`)
         |COMMENT 'DeltaOMS External Location'""".stripMargin
        .replaceAll("\n", " ")
      )
    assert(extLocQuery._2 == "EXTERNAL LOCATION")
  }

  test("Valid Catalog Creation SQL") {
    val testConfig = OMSConfig(catalogName = Some("deltaoms"),
      locationUrl = Some("/deltaoms/deltaoms"))
    val catQuery = UtilityOperations.catalogCreationQuery(omsCatalogDefinition(testConfig))
    assert(catQuery._1 ==
      """CREATE CATALOG IF NOT EXISTS `deltaoms`
        |MANAGED LOCATION '/deltaoms/deltaoms/deltaoms'
        |COMMENT 'DeltaOMS Catalog'""".stripMargin.replaceAll("\n", " ")
    )
    assert(catQuery._2 == "CATALOG")
  }

  test("Valid Schema Creation SQL") {
    val testConfig = OMSConfig(catalogName = Some("deltaoms"),
      schemaName = Some("deltaoms_test"),
      locationUrl = Some("/deltaoms/deltaoms"))
    spark.conf.set("spark.databricks.labs.deltaoms.ucenabled", value = true)
    val schemaQuery = UtilityOperations.schemaCreationQuery(omsSchemaDefinition(testConfig,
      Some(Map("entity" -> "oms", "oms.version" -> "0.5.0"))))
    assert(schemaQuery._1 ==
      """CREATE SCHEMA IF NOT EXISTS deltaoms.`deltaoms_test`
        |MANAGED LOCATION '/deltaoms/deltaoms/deltaoms/deltaoms_test'
        |COMMENT 'DeltaOMS Schema'
        |WITH DBPROPERTIES('entity'='oms','oms.version'='0.5.0')"""
        .stripMargin.replaceAll("\n", " ")
    )
    assert(schemaQuery._2 == "SCHEMA")
    spark.conf.set("spark.databricks.labs.deltaoms.ucenabled", value = false)
  }

  test("ucenabled check") {
    assert(!isUCEnabled)
  }

  test("Valid Database Creation SQL") {
    val testConfig = OMSConfig(schemaName = Some("deltaoms_test"),
      locationUrl = Some("//databricks-deltaoms/deltaoms"))
    val schemaQuery = UtilityOperations.schemaCreationQuery(omsSchemaDefinition(testConfig,
      Some(Map("entity" -> "oms", "oms.version" -> "0.5.0"))))
    assert(schemaQuery._1 ==
      """CREATE DATABASE IF NOT EXISTS deltaoms_test
        |LOCATION '//databricks-deltaoms/deltaoms/deltaoms_test'
        |COMMENT 'DeltaOMS Schema'
        |WITH DBPROPERTIES('entity'='oms','oms.version'='0.5.0')"""
        .stripMargin.replaceAll("\n", " ")
    )
    assert(schemaQuery._2 == "DATABASE")
  }

  test("Valid All Table Creation SQL") {
    val testConfig = OMSConfig(catalogName = Some("deltaoms"),
      schemaName = Some("deltaoms_test"),
      locationUrl = Some("/deltaoms/deltaoms"))

    // Source Config Table
    val srcConfigTableQuery = UtilityOperations.tableCreateQuery(sourceConfigDefinition(testConfig))
    assert(srcConfigTableQuery._1 ==
      s"CREATE TABLE IF NOT EXISTS deltaoms.`deltaoms_test`" +
        s".`sourceconfig` (${Schemas.sourceConfig.toDDL}) " +
        s"LOCATION '/deltaoms/deltaoms/deltaoms/deltaoms_test/sourceconfig' " +
        s"COMMENT 'Delta OMS Source Config Table'"
    )
    assert(srcConfigTableQuery._2 == "TABLE")
    // Path Config Table
    val pathConfigTableQuery = UtilityOperations
      .tableCreateQuery(pathConfigTableDefinition(testConfig))
    assert(pathConfigTableQuery._1 == s"CREATE TABLE IF NOT EXISTS " +
      s"${testConfig.catalogName.get}.`${testConfig.schemaName.get}`" +
      s".`${testConfig.pathConfigTable}` (${Schemas.pathConfig.toDDL}) " +
      s"LOCATION '/deltaoms/deltaoms/deltaoms/deltaoms_test/pathconfig' " +
      s"COMMENT 'Delta OMS Path Config Table'"
    )
    // Raw Actions Table
    val rawActionsTableQuery = UtilityOperations
      .tableCreateQuery(rawActionsTableDefinition(testConfig))
    assert(rawActionsTableQuery._1 == s"CREATE TABLE IF NOT EXISTS ${testConfig.catalogName.get}" +
      s".`${testConfig.schemaName.get}`.`${testConfig.rawActionTable}` " +
      s"(${Schemas.rawAction.toDDL}) " +
      s"PARTITIONED BY (${puidCommitDatePartitions.mkString(",")}) " +
      s"LOCATION '/deltaoms/deltaoms/deltaoms/deltaoms_test/rawactions' " +
      s"COMMENT 'Delta OMS Raw Actions Table'"
    )
    // Processing History Table
    val processingHistoryTableQuery = UtilityOperations
      .tableCreateQuery(processedHistoryTableDefinition(testConfig))
    assert(processingHistoryTableQuery._1 == s"CREATE TABLE IF NOT EXISTS " +
      s"${testConfig.catalogName.get}" +
      s".`${testConfig.schemaName.get}`.`${testConfig.processedHistoryTable}` " +
      s"(${Schemas.processedHistory.toDDL}) " +
      s"LOCATION '/deltaoms/deltaoms/deltaoms/deltaoms_test/processedhistory' " +
      s"COMMENT 'Delta OMS Processed History Table'"
    )
  }
}
