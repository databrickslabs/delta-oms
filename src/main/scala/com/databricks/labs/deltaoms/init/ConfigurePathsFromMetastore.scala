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

package com.databricks.labs.deltaoms.init

import com.databricks.labs.deltaoms.common.BatchOMSRunner

object ConfigurePathsFromMetastore extends BatchOMSRunner {

  def main(args: Array[String]): Unit = {
    spark.conf.set("spark.databricks.labs.deltaoms.class", value = getClass.getCanonicalName)
    val ucEnabled: Boolean = spark.conf.getOption("spark.databricks.labs.deltaoms.ucenabled")
      .fold(true)(_.toBoolean)

    val consolidatedOMSConfig = consolidateOMSConfig()
    logInfo(s"Starting Delta table path configuration update for OMS with Configuration : " +
      s"$consolidatedOMSConfig")
    // Create the OMS Database and Path Config Table Structures , if needed
    if(!consolidatedOMSConfig.skipInitializeOMS) {
      initializeOMS(consolidatedOMSConfig, ucEnabled = ucEnabled)
    }

    // Fetch the latest metastore delta tables and update the Path in the config table
    updateOMSPathConfigFromMetaStore(consolidatedOMSConfig)
  }
}
