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

import io.delta.oms.model.OMSCommandLineArgs

object OMSCommandLineParser {

  final val switchPattern = "(--[^=]+)".r
  final val keyValuePattern = "(--[^=]+)=(.+)".r
  final val SKIP_PATH_CONFIG = "--skipPathConfig"
  final val SKIP_INITIALIZE_OMS = "--skipInitializeOMS"
  final val USE_WILDCARD_PATHS = "--useWildcardPaths"

  def parseOMSCommandArgs(args: Array[String]): OMSCommandLineArgs = {
    val parsedCommandArgs: Array[(String, String)] = args.map(arg => arg match {
      case switchPattern(k) => (k, "true")
      case keyValuePattern(k, v) => (k, v)
      case _ => throw new RuntimeException("Unknown OMS Command Line Options")
    })
    parsedCommandArgs.foldLeft(OMSCommandLineArgs()) {
      (omsCommandArgs, argOptionValue) => {
        argOptionValue match {
          case (option, value) =>
            option match {
              case SKIP_PATH_CONFIG => omsCommandArgs.copy(skipPathConfig = true)
              case SKIP_INITIALIZE_OMS => omsCommandArgs.copy(skipInitializeOMS = true)
              case USE_WILDCARD_PATHS => omsCommandArgs.copy(consolidatedWildCardPaths = true)
            }
        }
      }
    }
  }
}
