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
package com.databricks.labs.deltaoms.utils

import com.databricks.labs.deltaoms.common.OMSInitializer
import com.databricks.labs.deltaoms.configuration.ConfigurationSettings
import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.delta.test.DeltaTestSharedSession
import org.apache.spark.sql.test.SharedSparkSession

class UtilityOperationsSuite extends QueryTest with SharedSparkSession with DeltaTestSharedSession
  with BeforeAndAfter with ConfigurationSettings with OMSInitializer {
  import testImplicits._
  // scalastyle:on funsuite

  test("Consolidate WildcardPaths") {
    val wcPaths1 = Array(("file:/tmp/oms/*/*/_delta_log/*.json", "abcd"),
      ("file:/tmp/oms/test_db_jun16/*/_delta_log/*.json", "efgh"))
    assert(UtilityOperations.consolidateWildCardPaths(wcPaths1).size == 1)

    val wcPaths2 = Array(
      ("file:/home/user/oms/test_db_jun16/*/_delta_log/*.json", "efgh"),
      ("file:/home/user/oms/*/*/_delta_log/*.json", "abcd"))
    assert(UtilityOperations.consolidateWildCardPaths(wcPaths1).size == 1)
  }
}
