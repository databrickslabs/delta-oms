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

package com.databricks.labs.deltaoms.utils

import org.apache.spark.sql.Dataset

object DataFrameOperations {
  implicit class DataFrameOps[T](val logData: Dataset[T]) extends AnyVal {
    def writeDataToDeltaTable(path: String,
                              mode: String = "append",
                              partitionColumnNames: Seq[String] = Seq.empty[String]): Unit = {
      val dw = logData.write.mode(mode).format("delta")
      if(partitionColumnNames.nonEmpty) {
        dw.partitionBy(partitionColumnNames: _*).save(path)
      } else {
        dw.save(path)
      }
    }
  }
}
