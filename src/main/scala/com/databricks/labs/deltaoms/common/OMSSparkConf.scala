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

import com.databricks.labs.deltaoms.configuration.{OMSConfig, SparkSettings}

trait OMSSparkConf extends Serializable with SparkSettings {

  def buildConfKey(key: String): String = s"databricks.labs.deltaoms.${key}"

  val BASE_LOCATION = buildConfKey("base.location")
  val DB_NAME = buildConfKey("db.name")
  val CHECKPOINT_BASE = buildConfKey("checkpoint.base")
  val CHECKPOINT_SUFFIX = buildConfKey("checkpoint.suffix")

  def consolidateOMSConfigFromSparkConf(config: OMSConfig): OMSConfig = {
    Seq(BASE_LOCATION, DB_NAME, CHECKPOINT_BASE, CHECKPOINT_SUFFIX).foldLeft(config) {
      (omsSparkConfig, configValue) => {
        configValue match {
          case BASE_LOCATION => spark.conf.getOption(BASE_LOCATION).fold(omsSparkConfig) {
            sprkVal => omsSparkConfig.copy(baseLocation = Some(sprkVal))
          }
          case DB_NAME => spark.conf.getOption(DB_NAME).fold(omsSparkConfig) {
            sprkVal => omsSparkConfig.copy(dbName = Some(sprkVal))
          }
          case CHECKPOINT_BASE => spark.conf.getOption(CHECKPOINT_BASE).fold(omsSparkConfig) {
            sprkVal => omsSparkConfig.copy(checkpointBase = Some(sprkVal))
          }
          case CHECKPOINT_SUFFIX => spark.conf.getOption(CHECKPOINT_SUFFIX).fold(omsSparkConfig) {
            sprkVal => omsSparkConfig.copy(checkpointSuffix = Some(sprkVal))
          }
        }
      }
    }
  }
}

object OMSSparkConf extends OMSSparkConf
