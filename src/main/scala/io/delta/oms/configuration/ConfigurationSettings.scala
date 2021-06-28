/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.oms.configuration

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
