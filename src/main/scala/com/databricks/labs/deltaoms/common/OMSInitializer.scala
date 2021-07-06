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
import OMSUtils._
import com.databricks.labs.deltaoms.utils.UtilityOperations._

import org.apache.spark.internal.Logging

trait OMSInitializer extends Serializable with Logging {

  def initializeOMSPathConfig(config: OMSConfig, dropAndRecreate: Boolean = false): Unit = {
    if (dropAndRecreate) {
      cleanupOMS(config)
    }
    createOMSDB(config)
    createPathConfigTables(config)
  }

  def initializeOMS(config: OMSConfig, dropAndRecreate: Boolean = false): Unit = {
    if (dropAndRecreate) {
      cleanupOMS(config)
    }
    createOMSDB(config)
    createOMSTables(config)
    /* Uncomment to add the new created OMS Database to be monitored by OMS
    if (dropAndRecreate) {
      populateOMSSourceConfigTableWithSelf(config.dbName, config.sourceConfigTable)
    } */
  }

  def createOMSDB(config: OMSConfig): Unit = {
    logInfo("Creating the OMS Database on Delta Lake")
    createDatabaseIfAbsent(omsDatabaseDefinition(config))
  }

  def createOMSTables(config: OMSConfig): Unit = {
    logInfo("Creating the EXTERNAL Source Config table on OMS Delta Lake")
    createTableIfAbsent(sourceConfigDefinition(config))
    logInfo("Creating the INTERNAL Path Config table on OMS Delta Lake")
    createPathConfigTables(config)
    logInfo("Creating the Delta Raw Actions table on OMS Delta Lake")
    createTableIfAbsent(rawActionsTableDefinition(config))
    logInfo("Creating the Processing History table on OMS Delta Lake")
    createTableIfAbsent(processedHistoryTableDefinition(config))
  }

  def createPathConfigTables(config: OMSConfig): Unit = {
    logInfo("Creating the Delta Table Path Config Table on Delta OMS")
    createTableIfAbsent(pathConfigTableDefinition(config))
  }

  def cleanupOMS(config: OMSConfig): Unit = {
    val deleteDBPath = Try {
      deleteDirectory(getOMSDBPath(config))
    }
    deleteDBPath match {
      case Success(value) => logInfo(s"Successfully deleted the directory ${getOMSDBPath(config)}")
      case Failure(exception) => throw exception
    }
    val dbDrop = Try {
      dropDatabase(config.dbName.get)
    }
    dbDrop match {
      case Success(value) => logInfo(s"Successfully dropped OMS database ${config.dbName}")
      case Failure(exception) => throw exception
    }
  }
}
