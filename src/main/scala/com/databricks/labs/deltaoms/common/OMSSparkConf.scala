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

import com.databricks.labs.deltaoms.configuration.{OMSConfig, SparkSettings}
// TODO - Modify as per suggestion here :
//  https://github.com/databrickslabs/delta-oms/pull/17#discussion_r721681401
trait OMSSparkConf extends Serializable with SparkSettings {

  def buildConfKey(key: String): String = s"databricks.labs.deltaoms.${key}"

  // Mandatory Configurations
  val BASE_LOCATION = buildConfKey("base.location")
  val DB_NAME = buildConfKey("db.name")
  val CHECKPOINT_BASE = buildConfKey("checkpoint.base")
  val CHECKPOINT_SUFFIX = buildConfKey("checkpoint.suffix")

  // Optional Configuration
  val RAW_ACTION_TABLE = buildConfKey("raw.action.table")
  val SOURCE_CONFIG_TABLE = buildConfKey("source.config.table")
  val PATH_CONFIG_TABLE = buildConfKey("path.config.table")
  val PROCESSED_HISTORY_TABLE = buildConfKey("processed.history.table")
  val COMMITINFO_SNAPSHOT_TABLE = buildConfKey("commitinfo.snapshot.table")
  val ACTION_SNAPSHOT_TABLE = buildConfKey("action.snapshot.table")
  val CONSOLIDATE_WILDCARD_PATHS = buildConfKey("consolidate.wildcard.paths")
  val TRUNCATE_PATH_CONFIG = buildConfKey("truncate.path.config")
  val SKIP_PATH_CONFIG = buildConfKey("skip.path.config")
  val SKIP_INITIALIZE = buildConfKey("skip.initialize")
  val SRC_DATABASES = buildConfKey("src.databases")
  val TABLE_PATTERN = buildConfKey("table.pattern")
  val TRIGGER_INTERVAL = buildConfKey("trigger.interval")
  val STARTING_STREAM = buildConfKey("starting.stream")
  val ENDING_STREAM = buildConfKey("ending.stream")


  val configFields = Seq(BASE_LOCATION, DB_NAME, CHECKPOINT_BASE, CHECKPOINT_SUFFIX,
    RAW_ACTION_TABLE, SOURCE_CONFIG_TABLE, PATH_CONFIG_TABLE, PROCESSED_HISTORY_TABLE,
    COMMITINFO_SNAPSHOT_TABLE, ACTION_SNAPSHOT_TABLE, CONSOLIDATE_WILDCARD_PATHS,
    TRUNCATE_PATH_CONFIG, SKIP_PATH_CONFIG, SKIP_INITIALIZE, SRC_DATABASES,
    TABLE_PATTERN, TRIGGER_INTERVAL, STARTING_STREAM, ENDING_STREAM)

  def consolidateOMSConfigFromSparkConf(config: OMSConfig): OMSConfig = {
    configFields.foldLeft(config) {
      (omsSparkConfig, configValue) => {
        configValue match {
          case BASE_LOCATION => spark.conf.getOption(BASE_LOCATION).
            fold(omsSparkConfig) { scv => omsSparkConfig.copy(baseLocation = Some(scv))}
          case DB_NAME => spark.conf.getOption(DB_NAME).
            fold(omsSparkConfig) { scv => omsSparkConfig.copy(dbName = Some(scv))}
          case CHECKPOINT_BASE => spark.conf.getOption(CHECKPOINT_BASE).
            fold(omsSparkConfig) { scv => omsSparkConfig.copy(checkpointBase = Some(scv))}
          case CHECKPOINT_SUFFIX => spark.conf.getOption(CHECKPOINT_SUFFIX).
            fold(omsSparkConfig) { scv => omsSparkConfig.copy(checkpointSuffix = Some(scv))}
          case RAW_ACTION_TABLE => spark.conf.getOption(RAW_ACTION_TABLE).
            fold(omsSparkConfig) { scv => omsSparkConfig.copy(rawActionTable = scv)}
          case SOURCE_CONFIG_TABLE => spark.conf.getOption(SOURCE_CONFIG_TABLE).
            fold(omsSparkConfig) { scv => omsSparkConfig.copy(sourceConfigTable = scv)}
          case PATH_CONFIG_TABLE => spark.conf.getOption(PATH_CONFIG_TABLE).
            fold(omsSparkConfig) { scv => omsSparkConfig.copy(pathConfigTable = scv)}
          case PROCESSED_HISTORY_TABLE => spark.conf.getOption(PROCESSED_HISTORY_TABLE).
            fold(omsSparkConfig) { scv => omsSparkConfig.copy(processedHistoryTable = scv)}
          case COMMITINFO_SNAPSHOT_TABLE => spark.conf.getOption(COMMITINFO_SNAPSHOT_TABLE).
            fold(omsSparkConfig) { scv => omsSparkConfig.copy(commitInfoSnapshotTable = scv)}
          case ACTION_SNAPSHOT_TABLE => spark.conf.getOption(ACTION_SNAPSHOT_TABLE).
            fold(omsSparkConfig) { scv => omsSparkConfig.copy(actionSnapshotTable = scv)}
          case CONSOLIDATE_WILDCARD_PATHS => spark.conf.getOption(CONSOLIDATE_WILDCARD_PATHS).
            fold(omsSparkConfig) {
              scv => omsSparkConfig.copy(consolidateWildcardPaths = scv.toBoolean)}
          case TRUNCATE_PATH_CONFIG => spark.conf.getOption(TRUNCATE_PATH_CONFIG).
            fold(omsSparkConfig) {
              scv => omsSparkConfig.copy(truncatePathConfig = scv.toBoolean)}
          case SKIP_PATH_CONFIG => spark.conf.getOption(SKIP_PATH_CONFIG).
            fold(omsSparkConfig) {
              scv => omsSparkConfig.copy(skipPathConfig = scv.toBoolean)}
          case SKIP_INITIALIZE => spark.conf.getOption(SKIP_INITIALIZE).
            fold(omsSparkConfig) {
              scv => omsSparkConfig.copy(skipInitializeOMS = scv.toBoolean)}
          case SRC_DATABASES => spark.conf.getOption(SRC_DATABASES).
            fold(omsSparkConfig) { scv => omsSparkConfig.copy(srcDatabases = Some(scv))}
          case TABLE_PATTERN => spark.conf.getOption(TABLE_PATTERN).
            fold(omsSparkConfig) { scv => omsSparkConfig.copy(tablePattern = Some(scv))}
          case TRIGGER_INTERVAL => spark.conf.getOption(TRIGGER_INTERVAL).
            fold(omsSparkConfig) { scv => omsSparkConfig.copy(triggerInterval = Some(scv))}
          case STARTING_STREAM => spark.conf.getOption(STARTING_STREAM).
            fold(omsSparkConfig) { scv => omsSparkConfig.copy(startingStream = scv.toInt)}
          case ENDING_STREAM => spark.conf.getOption(ENDING_STREAM).
            fold(omsSparkConfig) { scv => omsSparkConfig.copy(endingStream = scv.toInt)}
        }
      }
    }
  }
}

object OMSSparkConf extends OMSSparkConf
