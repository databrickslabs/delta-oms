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

import com.databricks.labs.deltaoms.configuration.OMSConfig
import com.databricks.labs.deltaoms.model.{DatabaseDefinition, TableDefinition}

import org.apache.spark.internal.Logging

trait Utils extends Serializable with Logging with Schemas {

  def getOMSDBPath(config: OMSConfig): String =
    s"${config.baseLocation.get}/${config.dbName.get}"
  def getRawActionsTableName(config: OMSConfig): String =
    s"${config.dbName.get}.${config.rawActionTable}"
  def getRawActionsTablePath(config: OMSConfig): String =
    s"${getOMSDBPath(config)}/${config.rawActionTable}/"
  def getPathConfigTablePath(config: OMSConfig): String =
    s"${getOMSDBPath(config)}/${config.pathConfigTable}/"
  def getSourceConfigTablePath(config: OMSConfig): String =
    s"${getOMSDBPath(config)}/${config.sourceConfigTable}/"
  def getProcessedHistoryTablePath(config: OMSConfig): String =
    s"${getOMSDBPath(config)}/${config.processedHistoryTable}/"
  def getCommitSnapshotTablePath(config: OMSConfig): String =
    s"${getOMSDBPath(config)}/${config.commitInfoSnapshotTable}/"
  def getCommitSnapshotTableName(config: OMSConfig): String =
    s"${config.dbName.get}.${config.commitInfoSnapshotTable}"
  def getActionSnapshotTablePath(config: OMSConfig): String =
    s"${getOMSDBPath(config)}/${config.actionSnapshotTable}/"
  def getActionSnapshotTableName(config: OMSConfig): String =
    s"${config.dbName.get}.${config.actionSnapshotTable}"

  val puidCommitDatePartitions = Seq(PUID, COMMIT_DATE)

  private val omsProperties = Map("entity" -> s"$ENTITY_NAME", "oms.version" -> s"$OMS_VERSION")

  def pathConfigTableDefinition(omsConfig: OMSConfig): TableDefinition = {
    TableDefinition(omsConfig.pathConfigTable,
      omsConfig.dbName.get,
      pathConfig,
      getPathConfigTablePath(omsConfig),
      Some("Delta OMS Path Config Table"),
      omsProperties
    )
  }

  def sourceConfigDefinition(omsConfig: OMSConfig): TableDefinition = {
    TableDefinition(omsConfig.sourceConfigTable,
      omsConfig.dbName.get,
      sourceConfig,
      getSourceConfigTablePath(omsConfig),
      Some("Delta OMS Source Config Table"),
      omsProperties
    )
  }

  def rawActionsTableDefinition(omsConfig: OMSConfig): TableDefinition = {
    TableDefinition(omsConfig.rawActionTable,
      omsConfig.dbName.get,
      rawAction,
      getRawActionsTablePath(omsConfig),
      Some("Delta OMS Raw Actions Table"),
      omsProperties,
      puidCommitDatePartitions)
  }

  def processedHistoryTableDefinition(omsConfig: OMSConfig): TableDefinition = {
    TableDefinition(omsConfig.processedHistoryTable,
      omsConfig.dbName.get,
      processedHistory,
      getProcessedHistoryTablePath(omsConfig),
      Some("Delta OMS Processed History Table"),
      omsProperties
    )
  }

  def omsDatabaseDefinition(omsConfig: OMSConfig): DatabaseDefinition = {
    DatabaseDefinition(omsConfig.dbName.get,
      Some(getOMSDBPath(omsConfig)),
      Some("Delta OMS Database"),
      omsProperties
    )
  }
}

object Utils extends Utils
