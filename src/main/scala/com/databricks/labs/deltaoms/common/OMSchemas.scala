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

package com.databricks.labs.deltaoms.common

import com.databricks.labs.deltaoms.model.{PathConfig, ProcessedHistory, SourceConfig}

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.delta.actions.SingleAction
import org.apache.spark.sql.types._

trait OMSchemas {
  final val PATH = "path"
  final val PUID = "puid"
  final val WUID = "wuid"
  final val QUALIFIED_NAME = "qualifiedName"
  final val UPDATE_TS = "update_ts"
  final val COMMIT_DATE = "commit_date"
  final val COMMIT_TS = "commit_ts"
  final val COMMIT_VERSION = "commit_version"
  final val FILE_NAME = "file_name"
  final val WILDCARD_LEVEL = "wildCardLevel"
  final val WILDCARD_PATH = "wildCardPath"
  final val PARAMETERS = "parameters"
  final val SKIP_PROCESSING = "skipProcessing"

  final val rawAction = ScalaReflection.schemaFor[SingleAction].dataType.asInstanceOf[StructType]
    .add(StructField(FILE_NAME, StringType))
    .add(StructField(PATH, StringType))
    .add(StructField(PUID, StringType))
    .add(StructField(COMMIT_VERSION, LongType))
    .add(StructField(UPDATE_TS, TimestampType))
    .add(StructField(COMMIT_TS, TimestampType))
    .add(StructField(COMMIT_DATE, DateType))
  final val pathConfig = ScalaReflection.schemaFor[PathConfig].dataType.asInstanceOf[StructType]
  final val sourceConfig = ScalaReflection.schemaFor[SourceConfig].dataType.asInstanceOf[StructType]
  final val processedHistory = ScalaReflection.schemaFor[ProcessedHistory].dataType
    .asInstanceOf[StructType]
}

object OMSSchemas extends OMSchemas
