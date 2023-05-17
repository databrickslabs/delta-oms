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

package com.databricks.labs.deltaoms.process

import com.databricks.labs.deltaoms.common.BatchOMSRunner
import com.databricks.labs.deltaoms.common.Utils._

object OMSProcessRawActions extends BatchOMSRunner {

  def main(args: Array[String]): Unit = {
    spark.conf.set("spark.databricks.labs.deltaoms.class", value = getClass.getCanonicalName)
    val consolidatedOMSConfig = consolidateOMSConfig()
    logInfo(s"Starting processing the OMS Raw Data : $consolidatedOMSConfig")
    // Get the current version for Raw Actions
    val currentRawActionsVersion =
      getCurrentRawActionsVersion(getRawActionsTableUrl(consolidatedOMSConfig))
    // Get Last OMS Raw Actions Commit Version that was processed
    val lastProcessedRawActionsVersion =
      getLastProcessedRawActionsVersion(getProcessedHistoryTableUrl(consolidatedOMSConfig),
        consolidatedOMSConfig.rawActionTable)
    if (currentRawActionsVersion == 0 ||
      lastProcessedRawActionsVersion < currentRawActionsVersion) {
      // Read the changed data since that version
      val newRawActions = getUpdatedRawActions(lastProcessedRawActionsVersion,
        getRawActionsTableUrl(consolidatedOMSConfig))
      // Extract and Persist Commit Info from the new Raw Actions
      processCommitInfoFromRawActions(newRawActions,
        getCommitSnapshotsTableUrl(consolidatedOMSConfig))
      // Extract, Compute Version Snapshots and Persist Action Info from the new Raw Actions
      processActionSnapshotsFromRawActions(newRawActions,
        getActionSnapshotsTableUrl(consolidatedOMSConfig))
      // Find the latest version from the newly processed raw actions
      val latestRawActionVersion = getLatestRawActionsVersion(newRawActions)
      // Update history with the version
      updateLastProcessedRawActions(latestRawActionVersion,
        consolidatedOMSConfig.rawActionTable,
        getProcessedHistoryTableUrl(consolidatedOMSConfig))
    }
  }
}
