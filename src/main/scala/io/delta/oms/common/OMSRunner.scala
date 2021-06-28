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

package io.delta.oms.common

import io.delta.oms.configuration.{OMSConfig, SparkSettings}
import io.delta.oms.ingest.InitializeOMSTables.omsConfig

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
