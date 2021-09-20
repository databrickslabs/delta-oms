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

case class OMSConfig(baseLocation: Option[String] = None,
  dbName: Option[String] = None,
  checkpointBase: Option[String] = None,
  checkpointSuffix: Option[String] = None,
  rawActionTable: String = "rawactions",
  sourceConfigTable: String = "sourceconfig",
  pathConfigTable: String = "pathconfig",
  processedHistoryTable: String = "processedhistory",
  commitInfoSnapshotTable: String = "commitinfosnapshots",
  actionSnapshotTable: String = "actionsnapshots",
  consolidateWildcardPaths: Boolean = true,
  truncatePathConfig: Boolean = false,
  skipPathConfig: Boolean = false,
  skipInitializeOMS: Boolean = false,
  srcDatabases: Option[String] = None,
  tablePattern: Option[String] = None,
  triggerInterval: Option[String] = None,
  startingStream: Int = 1,
  endingStream: Int = 50)
