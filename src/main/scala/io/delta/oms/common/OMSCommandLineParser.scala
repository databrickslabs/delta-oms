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

import io.delta.oms.configuration.OMSConfig

object OMSCommandLineParser {

  final val SWITCH_PATTERN = "(--[^=]+)".r
  final val KEY_VALUE_PATTERN = "(--[^=]+)=(.+)".r
  final val SKIP_PATH_CONFIG = "--skipPathConfig"
  final val SKIP_INITIALIZE_OMS = "--skipInitializeOMS"
  final val USE_WILDCARD_PATHS = "--useWildcardPaths"
  final val DB_NAME = "--dbName"
  final val BASE_LOCATION = "--baseLocation"
  final val CHECKPOINT_BASE = "--checkpointBase"
  final val CHECKPOINT_SUFFIX = "--checkpointSuffix"

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
              case USE_WILDCARD_PATHS => omsCommandArgsConfig.copy(consolidateWildcardPaths = true)
              case DB_NAME => omsCommandArgsConfig.copy(dbName = Some(value))
              case BASE_LOCATION => omsCommandArgsConfig.copy(baseLocation = Some(value))
              case CHECKPOINT_BASE => omsCommandArgsConfig.copy(checkpointBase = Some(value))
              case CHECKPOINT_SUFFIX => omsCommandArgsConfig.copy(checkpointSuffix = Some(value))
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
