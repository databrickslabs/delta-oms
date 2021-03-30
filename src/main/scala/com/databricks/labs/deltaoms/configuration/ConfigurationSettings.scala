package com.databricks.labs.deltaoms.configuration

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.internal.Logging
import pureconfig.{ConfigObjectSource, ConfigSource}
import pureconfig.generic.auto._
import scala.io.BufferedSource
import scala.util.{Failure, Success, Try}

trait ConfigurationSettings extends Serializable with Logging {
  def environmentConfigFile: String =
    sys.props.getOrElse("OMS_CONFIG_FILE",
      sys.env.getOrElse("OMS_CONFIG_FILE","inbuilt")).toLowerCase

  def omsConfig: OMSConfig = configSource.loadOrThrow[OMSConfig]

  logInfo(s"Loading configuration from : ${environmentConfigFile}")
  logInfo(s"Environment set to : ${environment}")

  def configSource: ConfigObjectSource = environment match {
    case InBuilt => ConfigSource.default
    case Local => ConfigSource.string(fetchConfigFileContent(environmentConfigFile))
    case _ => ConfigSource.string(fetchConfigFileContent(environmentConfigFile))
  }

  def environment: Environment = EnvironmentResolver.fetchEnvironment(environmentConfigFile)

  def fetchConfigFileContent(fullFilePath: String): String = {
    val fileSystem = FileSystem.get(new URI(fullFilePath), new Configuration())
    val environmentConfigFilePath = new Path(fullFilePath)
    val configFileStream = Try {
      fileSystem.open(environmentConfigFilePath)
    }
    val configReadLines: BufferedSource = configFileStream match {
      case Success(value) => scala.io.Source.fromInputStream(value)
      case Failure(exception) => throw new RuntimeException(s"Error opening configuration file $fullFilePath. " +
        s"Exception: $exception")
    }
    configReadLines.mkString
  }

}
