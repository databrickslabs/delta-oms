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

case class OMSConfig(locationUrl: Option[String] = None,
  locationName: Option[String] = None,
  storageCredentialName: Option[String] = None,
  catalogName: Option[String] = None,
  schemaName: Option[String] = None,
  checkpointBase: Option[String] = None,
  checkpointSuffix: Option[String] = None,
  rawActionTable: String = "rawactions",
  sourceConfigTable: String = "sourceconfig",
  processedHistoryTable: String = "processedhistory",
  commitInfoSnapshotTable: String = "commitinfosnapshots",
  actionSnapshotTable: String = "actionsnapshots",
  consolidateWildcardPaths: Boolean = true,
  truncatePathConfig: Boolean = false,
  useAutoloader: Boolean = true,
  triggerInterval: Option[String] = None,
  maxFilesPerTrigger: String = "1024",
  startingStream: Int = 1,
  endingStream: Int = 50)
