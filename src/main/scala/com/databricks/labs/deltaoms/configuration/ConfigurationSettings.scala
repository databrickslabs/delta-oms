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

import org.apache.spark.internal.Logging

trait ConfigurationSettings extends Serializable with Logging {
  def omsConfig: OMSConfig = omsConfigSource

  def omsConfigSource: OMSConfig = environment match {
    case Empty => OMSConfig()
    case InBuilt => OMSConfig(baseLocation = Some("/tmp/spark-warehouse/oms.db"),
      dbName = Some("oms_default_inbuilt"),
      checkpointBase = Some("/tmp/_oms_checkpoints/"),
      checkpointSuffix = Some("_1"),
      rawActionTable = "raw_actions",
      sourceConfigTable = "source_config",
      pathConfigTable = "path_config",
      processedHistoryTable = "processed_history",
      commitInfoSnapshotTable = "commitinfo_snapshots",
      actionSnapshotTable = "action_snapshots")
  }

  def environment: Environment = EnvironmentResolver.fetchEnvironment(environmentType)

  def environmentType: String =
    sys.props.getOrElse("OMS_ENV",
      sys.env.getOrElse("OMS_ENV", "empty")).toLowerCase
}

object ConfigurationSettings extends ConfigurationSettings
