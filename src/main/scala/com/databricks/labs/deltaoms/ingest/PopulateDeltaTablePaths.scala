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

package com.databricks.labs.deltaoms.ingest

import com.databricks.labs.deltaoms.common.BatchOMSRunner

object PopulateDeltaTablePaths extends BatchOMSRunner {

  def main(args: Array[String]): Unit = {
    val consolidatedOMSConfig = consolidateAndValidateOMSConfig(args, omsConfig)
    logInfo(s"Starting Delta table path configuration update for OMS with Configuration : " +
      s"$consolidatedOMSConfig")
    // Create the OMS Database and Path Config Table Structures , if needed
    if(!consolidatedOMSConfig.skipInitializeOMS) {
      initializeOMS(consolidatedOMSConfig)
    }
    // Update the OMS Path Config from Table Config
    updateOMSPathConfigFromSourceConfig(consolidatedOMSConfig)
  }
}
