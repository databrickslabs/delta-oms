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

package org.apache.spark.sql.delta.test

import io.delta.sql.DeltaSparkSessionExtension

import org.apache.spark.sql.test.{SharedSparkSession, TestSparkSession}
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.internal.SQLConf

class DeltaSharedSparkSession(sparkConf: SparkConf) extends TestSparkSession(sparkConf) {
  override val extensions: SparkSessionExtensions = {
    val extensions = new SparkSessionExtensions
    new DeltaSparkSessionExtension().apply(extensions)
    extensions
  }
}

trait DeltaTestSharedSession { self: SharedSparkSession =>
    override protected def createSparkSession: TestSparkSession = {
      SparkSession.cleanupAnyExistingSession()
      val session = new DeltaSharedSparkSession(sparkConf)
      session.conf.set(SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key, classOf[DeltaCatalog].getName)
      // session.conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      session.conf.set(SQLConf.DEFAULT_DATA_SOURCE_NAME.key, "delta")
      session.conf.set(SQLConf.LEGACY_CREATE_HIVE_TABLE_BY_DEFAULT.key, value = false)
      session.conf.set("spark.databricks.labs.deltaoms.ucenabled", value = false)
      // session.conf.set(SQLConf.CONVERT_CTAS.key, value = true)
      session
    }
}

