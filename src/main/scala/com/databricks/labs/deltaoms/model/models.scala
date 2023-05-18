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

import org.apache.spark.sql.types.StructType

case class PathConfig(path: String,
  puid: String,
  wildCardPath: String,
  wuid: String,
  parameters: Map[String, String] = Map.empty[String, String],
  qualifiedName: Option[String] = None,
  skipProcessing: Boolean = false,
  update_ts: Instant = Instant.now())

case class SourceConfig(path: String, skipProcessing: Boolean = false)

case class ProcessedHistory(tableName: String, lastVersion: Long,
  update_ts: Instant = Instant.now())

case class TableDefinition(
  tableName: String,
  schemaName: String,
  catalogName: String,
  qualifiedSchemaName: String,
  locationUrl: String,
  schema: StructType,
  comment: Option[String] = None,
  properties: Map[String, String] = Map.empty[String, String],
  partitionColumnNames: Seq[String] = Seq.empty[String],
  version: Long = 0) {
  assert(schemaName.nonEmpty & tableName.nonEmpty,
    "Schema and Table Name are required")
}

case class ExternalLocationDefinition(locationName: String,
  locationUrl: String,
  storageCredentialName: String,
  comment: Option[String] = None) {
  assert(locationName.nonEmpty, "Location Name is required")
  assert(locationUrl.nonEmpty, "Location URL is required")
  assert(storageCredentialName.nonEmpty, "Storage Credential Name is required")
}

case class CatalogDefinition(catalogName: String,
  locationUrl: Option[String],
  comment: Option[String] = None) {
  assert(catalogName.nonEmpty, "Catalog Name is required")
}

case class SchemaDefinition(catalogName: String,
  schemaName: String,
  qualifiedSchemaName: String,
  locationUrl: Option[String],
  comment: Option[String] = None,
  properties: Map[String, String] = Map.empty) {
  assert(schemaName.nonEmpty, "Schema Name is required")
}

case class StreamTargetInfo(url: String, checkpointPath: String,
  wuid: Option[String] = None, puid: Option[String] = None)
