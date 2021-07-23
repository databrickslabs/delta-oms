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

import com.databricks.labs.deltaoms.configuration.OMSConfig

object OMSCommandLineParser {

  final val SWITCH_PATTERN = "(--[^=]+)".r
  final val KEY_VALUE_PATTERN = "(--[^=]+)=(.+)".r
  final val SKIP_PATH_CONFIG = "--skipPathConfig"
  final val SKIP_INITIALIZE_OMS = "--skipInitializeOMS"
  final val SKIP_WILDCARD_PATHS_CONSOLIDATION = "--skipWildcardPathsConsolidation"
  final val DB_NAME = "--dbName"
  final val BASE_LOCATION = "--baseLocation"
  final val CHECKPOINT_BASE = "--checkpointBase"
  final val CHECKPOINT_SUFFIX = "--checkpointSuffix"
  final val STARTING_STREAM = "--startingStream"
  final val ENDING_STREAM = "--endingStream"

  def consolidateAndValidateOMSConfig(args: Array[String], fileOMSConfig: OMSConfig,
    isBatch: Boolean = true): OMSConfig = {
    val consolidatedOMSConfig = parseCommandArgsAndConsolidateOMSConfig(args, fileOMSConfig)
    validateOMSConfig(consolidatedOMSConfig, isBatch)
    consolidatedOMSConfig
  }

  def parseCommandArgs(args: Array[String]): Array[(String, String)] = {
    args.map(arg => arg match {
      case SWITCH_PATTERN(k) => (k, "true")
      case KEY_VALUE_PATTERN(k, v) => (k, v)
      case _ => throw new RuntimeException("Unknown OMS Command Line Options")
    })
  }

  def parseCommandArgsAndConsolidateOMSConfig(args: Array[String], fileOMSConfig: OMSConfig):
  OMSConfig = {
    val parsedCommandArgs: Array[(String, String)] = parseCommandArgs(args)
    parsedCommandArgs.foldLeft(fileOMSConfig) {
      (omsCommandArgsConfig, argOptionValue) => {
        argOptionValue match {
          case (option, value) =>
            option match {
              case SKIP_PATH_CONFIG => omsCommandArgsConfig.copy(skipPathConfig = true)
              case SKIP_INITIALIZE_OMS => omsCommandArgsConfig.copy(skipInitializeOMS = true)
              case SKIP_WILDCARD_PATHS_CONSOLIDATION =>
                omsCommandArgsConfig.copy(consolidateWildcardPaths = false)
              case DB_NAME => omsCommandArgsConfig.copy(dbName = Some(value))
              case BASE_LOCATION => omsCommandArgsConfig.copy(baseLocation = Some(value))
              case CHECKPOINT_BASE => omsCommandArgsConfig.copy(checkpointBase = Some(value))
              case CHECKPOINT_SUFFIX => omsCommandArgsConfig.copy(checkpointSuffix = Some(value))
              case STARTING_STREAM => omsCommandArgsConfig.copy(startingStream = value.toInt)
              case ENDING_STREAM => omsCommandArgsConfig.copy(endingStream = value.toInt)
            }
        }
      }
    }
  }

  def validateOMSConfig(omsConfig: OMSConfig, isBatch: Boolean = true): Unit = {
    assert(omsConfig.baseLocation.isDefined,
      "Mandatory configuration OMS Base Location missing. " +
        "Provide through Command line argument --baseLocation or " +
        "through config file parameter base-location")
    assert(omsConfig.dbName.isDefined,
      "Mandatory configuration OMS DB Name missing. " +
        "Provide through Command line argument --dbName or " +
        "through config file parameter db-name")
    if(!isBatch) {
      assert(omsConfig.checkpointBase.isDefined,
        "Mandatory configuration OMS Checkpoint Base Location missing. " +
          "Provide through Command line argument --checkpointBase or " +
          "through config file parameter checkpoint-base")
      assert(omsConfig.checkpointSuffix.isDefined,
        "Mandatory configuration OMS Checkpoint Suffix missing. " +
          "Provide through Command line argument --checkpointSuffix or " +
          "through config file parameter checkpoint-suffix")
    }
  }
}
