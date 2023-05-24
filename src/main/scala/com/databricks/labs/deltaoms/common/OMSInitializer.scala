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

import scala.util.{Failure, Success, Try}
import com.databricks.labs.deltaoms.common.Utils._
import com.databricks.labs.deltaoms.configuration.OMSConfig
import com.databricks.labs.deltaoms.utils.UtilityOperations._

import org.apache.spark.internal.Logging

trait OMSInitializer extends Serializable with Logging {

  def initializeOMS(config: OMSConfig, dropAndRecreate: Boolean = false): Unit = {
    if (dropAndRecreate) {
      cleanupOMS(config)
    }
    createOMSSchema(config)
    createOMSTables(config)
  }

  def createOMSSchema(config: OMSConfig): Unit = {
    if (isUCEnabled) {
      logInfo("Creating the OMS Database Objects on UC")
      // CREATE EXTERNAL LOCATION
      val extLocQuery = externalLocationCreationQuery(omsExternalLocationDefinition(config))
      executeSQL(extLocQuery._1, extLocQuery._2)
      // CREATE CATALOG
      val catalogQuery = catalogCreationQuery(omsCatalogDefinition(config))
      executeSQL(catalogQuery._1, catalogQuery._2)
    }
    // CREATE SCHEMA / DATABASE
    logInfo("Creating the OMS Schema/Database")
    val schemaQuery = schemaCreationQuery(omsSchemaDefinition(config))
    executeSQL(schemaQuery._1, schemaQuery._2)
  }

  def createOMSTables(config: OMSConfig): Unit = {
    logInfo("Creating the Source Config table on OMS Delta Lake")
    val srcConfigTableQuery = tableCreateQuery(sourceConfigDefinition(config))
    executeSQL(srcConfigTableQuery._1, srcConfigTableQuery._2)

    logInfo("Creating the Path Config table on OMS Delta Lake")
    val pathConfigTableQuery = tableCreateQuery(pathConfigTableDefinition(config))
    executeSQL(pathConfigTableQuery._1, pathConfigTableQuery._2)

    logInfo("Creating the Delta Raw Actions table on OMS Delta Lake")
    val rawActionsTableQuery = tableCreateQuery(rawActionsTableDefinition(config))
    executeSQL(rawActionsTableQuery._1, rawActionsTableQuery._2)

    logInfo("Creating the Processing History table on OMS Delta Lake")
    val processedHistoryTableQuery = tableCreateQuery(processedHistoryTableDefinition(config))
    executeSQL(processedHistoryTableQuery._1, processedHistoryTableQuery._2)

    logInfo("Creating the ActionSnapshots table on OMS Delta Lake")
    val actionSnapshotsTableQuery = tableCreateQuery(actionSnapshotsTableDefinition(config))
    executeSQL(actionSnapshotsTableQuery._1, actionSnapshotsTableQuery._2)

    logInfo("Creating the CommitSnapshots table on OMS Delta Lake")
    val commitSnapshotsTableQuery = tableCreateQuery(commitSnapshotsTableDefinition(config))
    executeSQL(commitSnapshotsTableQuery._1, commitSnapshotsTableQuery._2)
  }

  def cleanupOMS(config: OMSConfig): Unit = {
    if (!isUCEnabled) {
      dropDatabase(config.schemaName.get)
    } else {
      dropCatalog(config.catalogName.get)
    }
    val delSchemaPath = getOMSSchemaPath(config)
    val deleteSchemaPath = Try {
      deleteDirectory(delSchemaPath)
    }
    deleteSchemaPath match {
      case Success(value) =>
        logInfo(s"Successfully deleted the directory ${getOMSSchemaPath(config)}")
      case Failure(exception) =>
        throw new RuntimeException(s"Unable to delete  $delSchemaPath : $exception")
    }
  }
}
