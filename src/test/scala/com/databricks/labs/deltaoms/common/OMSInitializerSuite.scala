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

import com.databricks.labs.deltaoms.configuration.{ConfigurationSettings, EnvironmentResolver}
import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.delta.test.DeltaTestSharedSession
import org.apache.spark.sql.test.SharedSparkSession

class OMSInitializerSuite extends QueryTest with SharedSparkSession with DeltaTestSharedSession
  with BeforeAndAfter with ConfigurationSettings with OMSInitializer{
  import testImplicits._
  // scalastyle:on funsuite

  test("Inbuilt Configuration Settings") {
    assert(environmentConfigFile == "inbuilt")
    assert(environment == EnvironmentResolver.fetchEnvironment("inbuilt"))
    assert(omsConfig.dbName.get == "oms_default_inbuilt")
  }

  test("Initialize OMS Database and tables") {
    val dbName = omsConfig.dbName.get
    assert(!spark.catalog.databaseExists(dbName))
    initializeOMS(omsConfig, dropAndRecreate = true)
    assert(spark.catalog.databaseExists(dbName))
    assert(spark.catalog.tableExists(dbName, omsConfig.sourceConfigTable))
    assert(spark.catalog.tableExists(dbName, omsConfig.pathConfigTable))
    assert(spark.catalog.tableExists(dbName, omsConfig.rawActionTable))
    assert(spark.catalog.tableExists(dbName, omsConfig.processedHistoryTable))
  }
}
