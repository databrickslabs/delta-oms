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

import java.util.Locale

import org.apache.spark.internal.Logging

trait ConfigurationSettings extends Serializable with Logging {
  def omsConfig: OMSConfig = omsConfigSource

  def omsConfigSource: OMSConfig = environment match {
    case Empty => OMSConfig()
    case InBuilt => OMSConfig(
      locationUrl = Some("/tmp/spark-warehouse/oms.db"),
      locationName = Some("inbuilt_location"),
      storageCredentialName = Some("inbuilt_storage_credential"),
      schemaName = Some("oms_default_inbuilt"),
      checkpointBase = Some("/tmp/_oms_checkpoints/"),
      checkpointSuffix = Some("_1"),
      rawActionTable = "raw_actions",
      sourceConfigTable = "source_config",
      pathConfigTable = "path_config",
      processedHistoryTable = "processed_history",
      commitInfoSnapshotTable = "commitinfo_snapshots",
      actionSnapshotTable = "action_snapshots")
  }

  def validateOMSConfig(omsConfig: OMSConfig, isBatch: Boolean = true): Unit = {
    assert(omsConfig.locationUrl.isDefined,
      "Mandatory configuration OMS Location URL missing")
    assert(omsConfig.locationName.isDefined,
      "Mandatory configuration OMS Location Name missing")
    assert(omsConfig.storageCredentialName.isDefined,
      "Mandatory configuration OMS Storage Credential Name missing")
    assert(omsConfig.catalogName.isDefined,
      "Mandatory configuration OMS Catalog Name missing")
    assert(omsConfig.schemaName.isDefined,
      "Mandatory configuration OMS Schema Name missing")
    if(!isBatch) {
      assert(omsConfig.checkpointBase.isDefined,
        "Mandatory configuration OMS Checkpoint Base Location missing")
      assert(omsConfig.checkpointSuffix.isDefined,
        "Mandatory configuration OMS Checkpoint Suffix missing")
    }
  }

  def environment: Environment = EnvironmentResolver.fetchEnvironment(environmentType)

  def environmentType: String =
    sys.props.getOrElse("OMS_ENV",
      sys.env.getOrElse("OMS_ENV", "empty")).toLowerCase(Locale.ROOT)
}

object ConfigurationSettings extends ConfigurationSettings
