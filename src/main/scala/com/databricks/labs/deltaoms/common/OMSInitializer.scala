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
import com.databricks.labs.deltaoms.configuration.OMSConfig
import Utils._
import com.databricks.labs.deltaoms.utils.UtilityOperations._

import org.apache.spark.internal.Logging

trait OMSInitializer extends Serializable with Logging {

  def initializeOMS(config: OMSConfig, dropAndRecreate: Boolean = false,
    ucEnabled: Boolean): Unit = {
    if (dropAndRecreate) {
      cleanupOMS(config, ucEnabled)
    }
    createOMSSchema(config, ucEnabled)
    if (!ucEnabled) createOMSTables(config) else createUCManagedOMSTables(config)
  }

  def createOMSSchema(config: OMSConfig, ucEnabled: Boolean): Unit = {
    if (ucEnabled) {
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
    val schemaQuery = schemaCreationQuery(omsSchemaDefinition(config), ucEnabled)
    executeSQL(schemaQuery._1, schemaQuery._2)
  }

  def createOMSTables(config: OMSConfig): Unit = {
    logInfo("Creating the EXTERNAL Source Config table on OMS Delta Lake")
    createTableIfAbsent(sourceConfigDefinition(config))
    logInfo("Creating the INTERNAL Path Config table on OMS Delta Lake")
    createTableIfAbsent(pathConfigTableDefinition(config))
    logInfo("Creating the Delta Raw Actions table on OMS Delta Lake")
    createTableIfAbsent(rawActionsTableDefinition(config))
    logInfo("Creating the Processing History table on OMS Delta Lake")
    createTableIfAbsent(processedHistoryTableDefinition(config))
  }

  def createUCManagedOMSTables(config: OMSConfig): Unit = {
    logInfo("Creating the EXTERNAL Source Config table on OMS Delta Lake")
    val srcConfigTableQuery = tableCreateQuery(sourceConfigDefinition(config))
    executeSQL(srcConfigTableQuery._1, srcConfigTableQuery._2)
    // createTableIfAbsent(sourceConfigDefinition(config))

    logInfo("Creating the INTERNAL Path Config table on OMS Delta Lake")
    val pathConfigTableQuery = tableCreateQuery(pathConfigTableDefinition(config))
    executeSQL(pathConfigTableQuery._1, pathConfigTableQuery._2)
    // createPathConfigTables(config)

    logInfo("Creating the Delta Raw Actions table on OMS Delta Lake")
    val rawActionsTableQuery = tableCreateQuery(rawActionsTableDefinition(config))
    executeSQL(rawActionsTableQuery._1, rawActionsTableQuery._2)
    // createTableIfAbsent(rawActionsTableDefinition(config))

    logInfo("Creating the Processing History table on OMS Delta Lake")
    val processedHistoryTableQuery = tableCreateQuery(processedHistoryTableDefinition(config))
    executeSQL(processedHistoryTableQuery._1, processedHistoryTableQuery._2)
    // createTableIfAbsent(processedHistoryTableDefinition(config))
  }

  def cleanupOMS(config: OMSConfig, ucEnabled: Boolean): Unit = {
    if (!ucEnabled) {
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
      dropDatabase(config.schemaName.get)
    } else {
      dropCatalog(config.catalogName.get)
    }
  }
}
