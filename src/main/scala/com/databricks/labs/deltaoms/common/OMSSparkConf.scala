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

import com.databricks.labs.deltaoms.common.OMSSparkConfUtils.{buildConfKey, getSparkConf}
import com.databricks.labs.deltaoms.configuration.{OMSConfig, SparkSettings}

private object OMSSparkConfUtils extends SparkSettings {
  def buildConfKey(key: String): String = s"spark.databricks.labs.deltaoms.$key"

  def getSparkConf[T](confKey: String): Option[String] = {
    spark.conf.getOption(confKey)
  }
}

case class WithSparkConf[T](value: T, sparkConfigName: String, is_required: Boolean = false)

case class OMSSparkConfig(
  // Mandatory Configurations
  locationUrl: WithSparkConf[Option[String]] = WithSparkConf(None,
    buildConfKey("location.url"), is_required = true),
  locationName: WithSparkConf[Option[String]] = WithSparkConf(None,
    buildConfKey("location.name"), is_required = true),
  storageCredentialName: WithSparkConf[Option[String]] = WithSparkConf(None,
    buildConfKey("storage.credential.name"), is_required = true),
  catalogName: WithSparkConf[Option[String]] = WithSparkConf(None,
    buildConfKey("catalog.name"), is_required = true),
  schemaName: WithSparkConf[Option[String]] = WithSparkConf(None,
    buildConfKey("schema.name"), is_required = true),
  checkpointBase: WithSparkConf[Option[String]] = WithSparkConf(None,
    buildConfKey("checkpoint.base"), is_required = true),
  checkpointSuffix: WithSparkConf[Option[String]] = WithSparkConf(None,
    buildConfKey("checkpoint.suffix"), is_required = true),

  // Optional Configuration
  rawActionTable: WithSparkConf[String] = WithSparkConf("rawactions",
    buildConfKey("raw.action.table")),
  sourceConfigTable: WithSparkConf[String] = WithSparkConf("sourceconfig",
    buildConfKey("source.config.table")),
  pathConfigTable: WithSparkConf[String] = WithSparkConf("pathconfig",
    buildConfKey("path.config.table")),
  processedHistoryTable: WithSparkConf[String] = WithSparkConf("processedhistory",
    buildConfKey("processed.history.table")),
  commitInfoSnapshotTable: WithSparkConf[String] = WithSparkConf("commitinfosnapshots",
    buildConfKey("commitinfo.snapshot.table")),
  actionSnapshotTable: WithSparkConf[String] = WithSparkConf("actionsnapshots",
    buildConfKey("action.snapshot.table")),
  consolidateWildcardPaths: WithSparkConf[Boolean] = WithSparkConf(true,
    buildConfKey("consolidate.wildcard.paths")),
  truncatePathConfig: WithSparkConf[Boolean] = WithSparkConf(false,
    buildConfKey("truncate.path.config")),
  skipPathConfig: WithSparkConf[Boolean] = WithSparkConf(false,
    buildConfKey("skip.path.config")),
  skipInitializeOMS: WithSparkConf[Boolean] = WithSparkConf(false,
    buildConfKey("skip.initialize")),
  useAutoloader: WithSparkConf[Boolean] = WithSparkConf(false,
    buildConfKey("use.autoloader")),
  srcDatabases: WithSparkConf[Option[String]] = WithSparkConf(None,
    buildConfKey("src.databases")),
  tablePattern: WithSparkConf[Option[String]] = WithSparkConf(None,
    buildConfKey("table.pattern")),
  maxFilesPerTrigger: WithSparkConf[Option[String]] = WithSparkConf(None,
    buildConfKey("trigger.max.files")),
  triggerInterval: WithSparkConf[Option[String]] = WithSparkConf(None,
    buildConfKey("trigger.interval")),
  startingStream: WithSparkConf[Int] = WithSparkConf(1,
    buildConfKey("starting.stream")),
  endingStream: WithSparkConf[Int] = WithSparkConf(50,
    buildConfKey("ending.stream")))

trait OMSSparkConf extends Serializable with SparkSettings {

  // Mandatory Configurations
  val LOCATION_URL: String = buildConfKey("location.url")
  val LOCATION_NAME: String = buildConfKey("location.name")
  val STORAGE_CREDENTIAL_NAME: String = buildConfKey("storage.credential.name")
  val CATALOG_NAME: String = buildConfKey("catalog.name")
  val SCHEMA_NAME: String = buildConfKey("schema.name")
  val CHECKPOINT_BASE: String = buildConfKey("checkpoint.base")
  val CHECKPOINT_SUFFIX: String = buildConfKey("checkpoint.suffix")

  // Optional Configuration
  val RAW_ACTION_TABLE: String = buildConfKey("raw.action.table")
  val SOURCE_CONFIG_TABLE: String = buildConfKey("source.config.table")
  val PATH_CONFIG_TABLE: String = buildConfKey("path.config.table")
  val PROCESSED_HISTORY_TABLE: String = buildConfKey("processed.history.table")
  val COMMITINFO_SNAPSHOT_TABLE: String = buildConfKey("commitinfo.snapshot.table")
  val ACTION_SNAPSHOT_TABLE: String = buildConfKey("action.snapshot.table")
  val CONSOLIDATE_WILDCARD_PATHS: String = buildConfKey("consolidate.wildcard.paths")
  val TRUNCATE_PATH_CONFIG: String = buildConfKey("truncate.path.config")
  val SKIP_PATH_CONFIG: String = buildConfKey("skip.path.config")
  val SKIP_INITIALIZE: String = buildConfKey("skip.initialize")
  val USE_AUTOLOADER: String = buildConfKey("use.autoloader")
  val SRC_DATABASES: String = buildConfKey("src.databases")
  val TABLE_PATTERN: String = buildConfKey("table.pattern")
  val TRIGGER_MAX_FILES: String = buildConfKey("trigger.max.files")
  val TRIGGER_INTERVAL: String = buildConfKey("trigger.interval")
  val STARTING_STREAM: String = buildConfKey("starting.stream")
  val ENDING_STREAM: String = buildConfKey("ending.stream")


  def consolidateOMSConfigFromSparkConf(config: OMSConfig): OMSConfig = {
    val sparkOmsConfMap = OMSSparkConfig()
    OMSConfig(
      locationUrl = getSparkConf(sparkOmsConfMap.locationUrl.sparkConfigName)
        .fold(config.locationUrl) {
          Some(_)
        },
      locationName = getSparkConf(sparkOmsConfMap.locationName.sparkConfigName)
        .fold(config.locationName) {
          Some(_)
        },
      storageCredentialName = getSparkConf(sparkOmsConfMap.storageCredentialName.sparkConfigName)
        .fold(config.storageCredentialName) {
          Some(_)
        },
      catalogName = getSparkConf(sparkOmsConfMap.catalogName.sparkConfigName)
        .fold(config.catalogName) {
          Some(_)
        },
      schemaName = getSparkConf(sparkOmsConfMap.schemaName.sparkConfigName)
        .fold(config.schemaName) {
          Some(_)
        },
      checkpointBase = getSparkConf(sparkOmsConfMap.checkpointBase.sparkConfigName)
        .fold(config.checkpointBase) {
          Some(_)
        },
      checkpointSuffix = getSparkConf(sparkOmsConfMap.checkpointSuffix.sparkConfigName)
        .fold(config.checkpointSuffix) {
          Some(_)
        },
      rawActionTable = getSparkConf(sparkOmsConfMap.rawActionTable.sparkConfigName)
        .fold(config.rawActionTable) {
          _.toString()
        },
      sourceConfigTable = getSparkConf(sparkOmsConfMap.sourceConfigTable.sparkConfigName)
        .fold(config.sourceConfigTable) {
          _.toString()
        },
      pathConfigTable = getSparkConf(sparkOmsConfMap.pathConfigTable.sparkConfigName)
        .fold(config.pathConfigTable) {
          _.toString()
        },
      processedHistoryTable = getSparkConf(sparkOmsConfMap.processedHistoryTable.sparkConfigName)
        .fold(config.processedHistoryTable) {
          _.toString()
        },
      commitInfoSnapshotTable =
        getSparkConf(sparkOmsConfMap.commitInfoSnapshotTable.sparkConfigName)
          .fold(config.commitInfoSnapshotTable) {
            _.toString()
          },
      actionSnapshotTable = getSparkConf(sparkOmsConfMap.actionSnapshotTable.sparkConfigName)
        .fold(config.actionSnapshotTable) {
          _.toString()
        },
      consolidateWildcardPaths =
        getSparkConf(sparkOmsConfMap.consolidateWildcardPaths.sparkConfigName)
          .fold(config.consolidateWildcardPaths) {
            _.toBoolean
          },
      truncatePathConfig = getSparkConf(sparkOmsConfMap.truncatePathConfig.sparkConfigName)
        .fold(config.truncatePathConfig) {
          _.toBoolean
        },
      useAutoloader = getSparkConf(sparkOmsConfMap.useAutoloader.sparkConfigName)
        .fold(config.useAutoloader) {
          _.toBoolean
        },
      maxFilesPerTrigger = getSparkConf(sparkOmsConfMap.maxFilesPerTrigger.sparkConfigName)
        .fold(config.maxFilesPerTrigger) {
          _.toString()
        },
      triggerInterval = getSparkConf(sparkOmsConfMap.triggerInterval.sparkConfigName)
        .fold(config.triggerInterval) {
          Some(_)
        },
      startingStream = getSparkConf(sparkOmsConfMap.startingStream.sparkConfigName)
        .fold(config.startingStream) {
          _.toInt
        },
      endingStream = getSparkConf(sparkOmsConfMap.endingStream.sparkConfigName)
        .fold(config.endingStream) {
          _.toInt
        })
  }
}

object OMSSparkConf extends OMSSparkConf
