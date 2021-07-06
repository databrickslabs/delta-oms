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

import com.databricks.labs.deltaoms.common.StreamOMSRunner

object StreamPopulateOMS extends StreamOMSRunner {

  def main(args: Array[String]): Unit = {

    val consolidatedOMSConfig = consolidateAndValidateOMSConfig(args, omsConfig)
    // Create the OMS Database and Table Structures , if needed
    if(!consolidatedOMSConfig.skipInitializeOMS) {
      initializeOMS(consolidatedOMSConfig)
    }
    // Update the OMS Path Config from Table Config
    if(!consolidatedOMSConfig.skipPathConfig) {
      updateOMSPathConfigFromSourceConfig(consolidatedOMSConfig)
    }
    logInfo(s"Starting Streaming OMS with Configuration : $consolidatedOMSConfig")
    // Streaming Ingest the Raw Actions for configured Delta tables
    streamingUpdateRawDeltaActionsToOMS(consolidatedOMSConfig)
  }
}
