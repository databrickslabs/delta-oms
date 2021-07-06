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

import org.apache.spark.internal.Logging


trait OMSRunner extends Serializable
  with SparkSettings
  with OMSInitializer
  with OMSOperations
  with Logging {

  logInfo(s"Loading configuration from : ${environmentConfigFile}")
  logInfo(s"Environment set to : ${environment}")
  logInfo(s"OMS Config from configuration file : ${omsConfig}")

  def consolidateAndValidateOMSConfig(args: Array[String], config: OMSConfig): OMSConfig
}

trait BatchOMSRunner extends OMSRunner {
  def consolidateAndValidateOMSConfig(args: Array[String], config: OMSConfig): OMSConfig = {
    OMSCommandLineParser.consolidateAndValidateOMSConfig(args, omsConfig)
  }
}

trait StreamOMSRunner extends OMSRunner{
  def consolidateAndValidateOMSConfig(args: Array[String], config: OMSConfig): OMSConfig = {
    OMSCommandLineParser.consolidateAndValidateOMSConfig(args, omsConfig, isBatch = false)
  }
}

