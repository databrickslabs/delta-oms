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

package com.databricks.labs.deltaoms.configuration

import com.databricks.labs.deltaoms.common.Schemas

import org.apache.spark.sql.SparkSession

trait SparkSettings extends Serializable with ConfigurationSettings {
  protected val sparkSession: SparkSession = environment match {
    case InBuilt => SparkSession.builder()
      .master("local")
      .config("spark.driver.host", "localhost")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .appName("DELTA_OMS_INBUILT").getOrCreate()
    case _ => val spark = SparkSession.builder().appName("Delta OMS").getOrCreate()
      spark.conf.set("spark.databricks.delta.properties.defaults.enableChangeDataFeed",
        value = true)
      spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", value = true)
      spark.conf.set("spark.databricks.delta.autoCompact.enabled", value = true)
      spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", value = true)
      spark.conf.set("spark.databricks.labs.deltaoms.version", value = Schemas.OMS_VERSION)
      spark
  }
  def spark: SparkSession = sparkSession

}
