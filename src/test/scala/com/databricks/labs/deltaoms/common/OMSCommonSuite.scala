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


import com.databricks.labs.deltaoms.configuration.{ConfigurationSettings, OMSConfig}
import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.delta.test.DeltaTestSharedSession
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.QueryTest

class OMSCommonSuite extends QueryTest with SharedSparkSession with DeltaTestSharedSession
  with BeforeAndAfter with ConfigurationSettings {

  test("Test Missing Fields Exceptions") {

    assert(intercept[java.lang.AssertionError] {
      validateOMSConfig(OMSConfig(schemaName = Some("abc"),
        catalogName = Some("cat"),
        locationName = Some("sampleLocation"),
        storageCredentialName = Some("sampleCredential"),
        checkpointBase = Some("/checkBase"),
        checkpointSuffix = Some("_checkSuffix_123")), isBatch = false)
    }.getMessage.contains("Mandatory configuration OMS Location URL missing"))

    assert(intercept[java.lang.AssertionError] {
      validateOMSConfig(OMSConfig(schemaName = Some("abc"),
        catalogName = Some("cat"),
        locationUrl = Some("/sampleBase"),
        storageCredentialName = Some("sampleCredential"),
        checkpointBase = Some("/checkBase"),
        checkpointSuffix = Some("_checkSuffix_123")), isBatch = false)
    }.getMessage.contains("Mandatory configuration OMS Location Name missing"))

    assert(intercept[java.lang.AssertionError] {
      validateOMSConfig(OMSConfig(schemaName = Some("abc"),
        catalogName = Some("cat"),
        locationUrl = Some("/sampleBase"),
        locationName = Some("sampleLocation"),
        checkpointBase = Some("/checkBase"),
        checkpointSuffix = Some("_checkSuffix_123")), isBatch = false)
    }.getMessage.contains("Mandatory configuration OMS Storage Credential Name missing"))

    assert(intercept[java.lang.AssertionError] {
      validateOMSConfig(OMSConfig(schemaName = Some("abc"),
        locationUrl = Some("/sampleBase"),
        locationName = Some("sampleLocation"),
        storageCredentialName = Some("sampleCredential"),
        checkpointBase = Some("/checkBase"),
        checkpointSuffix = Some("_checkSuffix_123")), isBatch = false)
    }.getMessage.contains("Mandatory configuration OMS Catalog Name missing"))

    assert(intercept[java.lang.AssertionError] {
      validateOMSConfig(OMSConfig(
        catalogName = Some("cat"),
        locationUrl = Some("/sampleBase"),
        locationName = Some("sampleLocation"),
        storageCredentialName = Some("sampleCredential"),
        checkpointBase = Some("/checkBase"),
        checkpointSuffix = Some("_checkSuffix_123")), isBatch = false)
    }.getMessage.contains("Mandatory configuration OMS Schema Name missing"))

    assert(intercept[java.lang.AssertionError] {
      validateOMSConfig(OMSConfig(schemaName = Some("abc"),
        catalogName = Some("cat"),
        locationUrl = Some("/sampleBase"),
        locationName = Some("sampleLocation"),
        storageCredentialName = Some("sampleCredential"),
        checkpointSuffix = Some("_checkSuffix_123")), isBatch = false)
    }.getMessage.contains("Mandatory configuration OMS Checkpoint Base Location missing"))

    assert(intercept[java.lang.AssertionError] {
      validateOMSConfig(OMSConfig(schemaName = Some("abc"),
        catalogName = Some("cat"),
        locationUrl = Some("/sampleBase"),
        locationName = Some("sampleLocation"),
        storageCredentialName = Some("sampleCredential"),
        checkpointBase = Some("/checkBase")), isBatch = false)
    }.getMessage.contains("Mandatory configuration OMS Checkpoint Suffix missing"))
  }

  test("Spark Optional Configurations checks") {
    val deltaOMSOptionalSparkConfigs = Seq(
      OMSSparkConf.RAW_ACTION_TABLE -> "test.rawactions",
      OMSSparkConf.SOURCE_CONFIG_TABLE -> "test.sourceconfig",
      OMSSparkConf.PATH_CONFIG_TABLE -> "test.pathconfig",
      OMSSparkConf.PROCESSED_HISTORY_TABLE -> "test.processhistory",
      OMSSparkConf.COMMITINFO_SNAPSHOT_TABLE -> "test.commitinfosnapshot",
      OMSSparkConf.ACTION_SNAPSHOT_TABLE -> "test.actionsnapshot",
      OMSSparkConf.CONSOLIDATE_WILDCARD_PATHS -> "false",
      OMSSparkConf.TRUNCATE_PATH_CONFIG -> "true",
      OMSSparkConf.TRIGGER_INTERVAL -> "30 sec",
      OMSSparkConf.TRIGGER_MAX_FILES -> "3000",
      OMSSparkConf.STARTING_STREAM -> "4",
      OMSSparkConf.ENDING_STREAM -> "10",
      OMSSparkConf.USE_AUTOLOADER -> "false")

    withSQLConf(deltaOMSOptionalSparkConfigs: _*) {
      val sparkOMSConfig = OMSSparkConf.consolidateOMSConfigFromSparkConf(OMSConfig())
      assert(sparkOMSConfig == OMSConfig(schemaName = None,
        locationUrl = None,
        locationName = None,
        storageCredentialName = None,
        catalogName = None,
        checkpointBase = None,
        checkpointSuffix = None,
        rawActionTable = "test.rawactions",
        sourceConfigTable = "test.sourceconfig",
        processedHistoryTable = "test.processhistory",
        commitInfoSnapshotTable = "test.commitinfosnapshot",
        actionSnapshotTable = "test.actionsnapshot",
        consolidateWildcardPaths = false,
        truncatePathConfig = true,
        useAutoloader = false,
        triggerInterval = Some("30 sec"),
        startingStream = 4,
        endingStream = 10,
        maxFilesPerTrigger = "3000"
      ))
    }
  }

  test("Spark Config Configuration provided") {
    val deltaOMSIngestionSparkConfigs = Seq(OMSSparkConf.LOCATION_URL -> "/sampleBase",
      OMSSparkConf.SCHEMA_NAME -> "abc",
      OMSSparkConf.CHECKPOINT_BASE -> "/checkBase",
      OMSSparkConf.CHECKPOINT_SUFFIX -> "_checkSuffix_123")

    withSQLConf(deltaOMSIngestionSparkConfigs: _*) {
      val sparkOMSConfig = OMSSparkConf.consolidateOMSConfigFromSparkConf(OMSConfig())
      assert(sparkOMSConfig == OMSConfig(schemaName = Some("abc"),
        locationUrl = Some("/sampleBase"),
        checkpointBase = Some("/checkBase"),
        checkpointSuffix = Some("_checkSuffix_123")))
    }
  }
}
