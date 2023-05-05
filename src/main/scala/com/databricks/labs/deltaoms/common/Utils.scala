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
import com.databricks.labs.deltaoms.model.{CatalogDefinition, ExternalLocationDefinition, SchemaDefinition, TableDefinition}

import org.apache.spark.internal.Logging

trait Utils extends Serializable with Logging with Schemas {

  def getOMSCatalogName(config: OMSConfig) : String = config.catalogName.getOrElse("")
  def getOMSSchemaName(config: OMSConfig) : String = config.schemaName.getOrElse("")
  def getOMSCatalogPath(config: OMSConfig): String = config.catalogName
    .fold(s"${config.locationUrl.get}") {c => s"${config.locationUrl.get}/${c}"}

  def getOMSSchemaPath(config: OMSConfig): String =
    s"${getOMSCatalogPath(config)}/${config.schemaName.get}"
  def getOMSQualifiedSchemaName(config: OMSConfig): String = config.catalogName
    .fold(s"${config.schemaName.get}") {c => s"$c.`${config.schemaName.get}`"}


  def getRawActionsTablePath(config: OMSConfig): String =
    s"${getOMSSchemaPath(config)}/${config.rawActionTable}/"
  def getPathConfigTablePath(config: OMSConfig): String =
    s"${getOMSSchemaPath(config)}/${config.pathConfigTable}/"
  def getSourceConfigTablePath(config: OMSConfig): String =
    s"${getOMSSchemaPath(config)}/${config.sourceConfigTable}/"
  def getProcessedHistoryTablePath(config: OMSConfig): String =
    s"${getOMSSchemaPath(config)}/${config.processedHistoryTable}/"
  def getCommitSnapshotTablePath(config: OMSConfig): String =
    s"${getOMSSchemaPath(config)}/${config.commitInfoSnapshotTable}/"
  def getActionSnapshotTablePath(config: OMSConfig): String =
    s"${getOMSSchemaPath(config)}/${config.actionSnapshotTable}/"

  def getRawActionsTableName(config: OMSConfig): String =
    s"${getOMSQualifiedSchemaName(config)}.`${config.rawActionTable}`"
  def getCommitSnapshotTableName(config: OMSConfig): String =
    s"${getOMSQualifiedSchemaName(config)}.`${config.commitInfoSnapshotTable}`"
  def getActionSnapshotTableName(config: OMSConfig): String =
    s"${getOMSQualifiedSchemaName(config)}.`${config.actionSnapshotTable}`"

  val puidCommitDatePartitions = Seq(PUID, COMMIT_DATE)

  private val omsProperties: Map[String, String] =
    Map("entity" -> s"$ENTITY_NAME", "oms.version" -> s"$OMS_VERSION")

  def pathConfigTableDefinition(omsConfig: OMSConfig): TableDefinition = {
    TableDefinition(omsConfig.pathConfigTable,
      getOMSSchemaName(omsConfig),
      getOMSCatalogName(omsConfig),
      getOMSQualifiedSchemaName(omsConfig),
      pathConfig,
      getPathConfigTablePath(omsConfig),
      Some("Delta OMS Path Config Table"),
      omsProperties
    )
  }

  def sourceConfigDefinition(omsConfig: OMSConfig): TableDefinition = {
    TableDefinition(omsConfig.sourceConfigTable,
      getOMSSchemaName(omsConfig),
      getOMSCatalogName(omsConfig),
      getOMSQualifiedSchemaName(omsConfig),
      sourceConfig,
      getSourceConfigTablePath(omsConfig),
      Some("Delta OMS Source Config Table"),
      omsProperties
    )
  }

  def rawActionsTableDefinition(omsConfig: OMSConfig): TableDefinition = {
    TableDefinition(omsConfig.rawActionTable,
      getOMSSchemaName(omsConfig),
      getOMSCatalogName(omsConfig),
      getOMSQualifiedSchemaName(omsConfig),
      rawAction,
      getRawActionsTablePath(omsConfig),
      Some("Delta OMS Raw Actions Table"),
      omsProperties,
      puidCommitDatePartitions)
  }

  def processedHistoryTableDefinition(omsConfig: OMSConfig): TableDefinition = {
    TableDefinition(omsConfig.processedHistoryTable,
      getOMSSchemaName(omsConfig),
      getOMSCatalogName(omsConfig),
      getOMSQualifiedSchemaName(omsConfig),
      processedHistory,
      getProcessedHistoryTablePath(omsConfig),
      Some("Delta OMS Processed History Table"),
      omsProperties
    )
  }

  def omsExternalLocationDefinition(omsConfig: OMSConfig): ExternalLocationDefinition = {
    ExternalLocationDefinition(omsConfig.locationName.get,
      omsConfig.locationUrl.get,
      omsConfig.storageCredentialName.get,
      Some("DeltaOMS External Location")
    )
  }

  def omsCatalogDefinition(omsConfig: OMSConfig): CatalogDefinition = {
    CatalogDefinition(getOMSCatalogName(omsConfig),
      Some(getOMSCatalogPath(omsConfig)),
      Some("DeltaOMS Catalog")
    )
  }

  def omsSchemaDefinition(omsConfig: OMSConfig,
    props: Option[Map[String, String]] = None): SchemaDefinition = {
    SchemaDefinition(getOMSCatalogName(omsConfig),
      getOMSSchemaName(omsConfig),
      getOMSQualifiedSchemaName(omsConfig),
      Some(getOMSSchemaPath(omsConfig)),
      Some("DeltaOMS Schema"),
      props.getOrElse(omsProperties)
    )
  }
}

object Utils extends Utils
