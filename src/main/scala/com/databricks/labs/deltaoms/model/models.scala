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

package com.databricks.labs.deltaoms.model

import java.time.Instant

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.CommitInfo
import org.apache.spark.sql.types.StructType

case class PathConfig(path: String,
  puid: String,
  wildCardPath: String,
  wuid: String,
  parameters: Map[String, String] = Map.empty[String, String],
  automated: Boolean = true,
  qualifiedName: Option[String] = None,
  commit_version: Long,
  skipProcessing: Boolean = false,
  update_ts: Instant = Instant.now()) {
  def getDeltaLog(spark: SparkSession): DeltaLog = {
    DeltaLog.forTable(spark, path)
  }
}

case class SourceConfig(path: String, skipProcessing: Boolean = false,
  parameters: Map[String, String] = Map.empty[String, String])

case class ProcessedHistory(tableName: String, lastVersion: Long,
  update_ts: Instant = Instant.now())

case class TableDefinition(
  tableName: String,
  databaseName: String = "default",
  schema: StructType,
  path: String,
  comment: Option[String] = None,
  properties: Map[String, String] = Map.empty[String, String],
  partitionColumnNames: Seq[String] = Seq.empty[String],
  version: Long = 0) {
  assert(path.nonEmpty & tableName.nonEmpty, "Table Name and Path is required")
}

case class DatabaseDefinition(databaseName: String,
  location: Option[String],
  comment: Option[String] = None,
  properties: Map[String, String] = Map.empty) {
  assert(databaseName.nonEmpty, "Database Name is required")
}

case class StreamTargetInfo(path: String, checkpointPath: String,
  wuid: Option[String] = None, puid: Option[String] = None)
