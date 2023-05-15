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

  def getSourceConfigTableName(config: OMSConfig): String =
    s"${getOMSQualifiedSchemaName(config)}.`${config.sourceConfigTable}`"
  def getPathConfigTableName(config: OMSConfig): String =
    s"${getOMSQualifiedSchemaName(config)}.`${config.pathConfigTable}`"
  def getRawActionsTableName(config: OMSConfig): String =
    s"${getOMSQualifiedSchemaName(config)}.`${config.rawActionTable}`"
  def getCommitSnapshotTableName(config: OMSConfig): String =
    s"${getOMSQualifiedSchemaName(config)}.`${config.commitInfoSnapshotTable}`"
  def getActionSnapshotTableName(config: OMSConfig): String =
    s"${getOMSQualifiedSchemaName(config)}.`${config.actionSnapshotTable}`"
  def getProcessedHistoryTableName(config: OMSConfig): String =
    s"${getOMSQualifiedSchemaName(config)}.`${config.processedHistoryTable}`"

  val puidCommitDatePartitions = Seq(PUID, COMMIT_DATE)

  private val omsProperties: Map[String, String] =
    Map("entity" -> s"$ENTITY_NAME", "oms.version" -> s"$OMS_VERSION")

  def pathConfigTableDefinition(omsConfig: OMSConfig): TableDefinition = {
    TableDefinition(tableName = omsConfig.pathConfigTable,
      schemaName = getOMSSchemaName(omsConfig),
      catalogName = getOMSCatalogName(omsConfig),
      qualifiedSchemaName = getOMSQualifiedSchemaName(omsConfig),
      schema = pathConfig,
      comment = Some("Delta OMS Path Config Table"),
      properties = omsProperties
    )
  }

  def sourceConfigDefinition(omsConfig: OMSConfig): TableDefinition = {
    TableDefinition(tableName = omsConfig.sourceConfigTable,
      schemaName = getOMSSchemaName(omsConfig),
      catalogName = getOMSCatalogName(omsConfig),
      qualifiedSchemaName = getOMSQualifiedSchemaName(omsConfig),
      schema = sourceConfig,
      comment = Some("Delta OMS Source Config Table"),
      properties = omsProperties
    )
  }

  def rawActionsTableDefinition(omsConfig: OMSConfig): TableDefinition = {
    TableDefinition(tableName = omsConfig.rawActionTable,
      schemaName = getOMSSchemaName(omsConfig),
      catalogName = getOMSCatalogName(omsConfig),
      qualifiedSchemaName = getOMSQualifiedSchemaName(omsConfig),
      schema = rawAction,
      comment = Some("Delta OMS Raw Actions Table"),
      properties = omsProperties,
      partitionColumnNames = puidCommitDatePartitions)
  }

  def processedHistoryTableDefinition(omsConfig: OMSConfig): TableDefinition = {
    TableDefinition(tableName = omsConfig.processedHistoryTable,
      schemaName = getOMSSchemaName(omsConfig),
      catalogName = getOMSCatalogName(omsConfig),
      qualifiedSchemaName = getOMSQualifiedSchemaName(omsConfig),
      schema = processedHistory,
      comment = Some("Delta OMS Processed History Table"),
      properties = omsProperties
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
