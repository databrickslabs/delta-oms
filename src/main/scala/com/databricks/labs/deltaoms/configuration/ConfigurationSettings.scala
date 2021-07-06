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

import java.net.URI

import scala.io.BufferedSource
import scala.util.{Failure, Success, Try}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import pureconfig.{ConfigObjectSource, ConfigSource}
import pureconfig.generic.auto._

import org.apache.spark.internal.Logging

trait ConfigurationSettings extends Serializable with Logging {
  def omsConfig: OMSConfig = configSource.loadOrThrow[OMSConfig]

  def configSource: ConfigObjectSource = environment match {
    case Empty => ConfigSource.empty
    case InBuilt => ConfigSource.default
    case Local => ConfigSource.string(fetchConfigFileContent(environmentConfigFile))
    case _ => ConfigSource.string(fetchConfigFileContent(environmentConfigFile))
  }

  def environment: Environment = EnvironmentResolver.fetchEnvironment(environmentConfigFile)

  def environmentConfigFile: String =
    sys.props.getOrElse("OMS_CONFIG_FILE",
      sys.env.getOrElse("OMS_CONFIG_FILE", "empty")).toLowerCase

  def fetchConfigFileContent(fullFilePath: String): String = {
    val fileSystem = FileSystem.get(new URI(fullFilePath), new Configuration())
    val environmentConfigFilePath = new Path(fullFilePath)
    val configFileStream = Try {
      fileSystem.open(environmentConfigFilePath)
    }
    val configReadLines: BufferedSource = configFileStream match {
      case Success(value) => scala.io.Source.fromInputStream(value)
      case Failure(exception) =>
        throw new RuntimeException(s"Error opening configuration file $fullFilePath. " +
          s"Exception: $exception")
    }
    configReadLines.mkString
  }

}

object ConfigurationSettings extends ConfigurationSettings
