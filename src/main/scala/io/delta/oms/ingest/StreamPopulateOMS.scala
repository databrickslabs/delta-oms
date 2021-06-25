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

package io.delta.oms.ingest

import io.delta.oms.common.{OMSCommandLineParser, OMSInitializer, OMSRunner}

object StreamPopulateOMS extends OMSRunner {

  def main(args: Array[String]): Unit = {
    logInfo(s"Starting Streaming OMS with Configuration : $omsConfig")
    val omsCommandArgs = OMSCommandLineParser.parseOMSCommandArgs(args)
    // Create the OMS Database and Table Structures , if needed
    if(!omsCommandArgs.skipInitializeOMS) {
      initializeOMS(omsConfig)
    }
    // Update the OMS Path Config from Table Config
    if(!omsCommandArgs.skipPathConfig) {
      updateOMSPathConfigFromSourceConfig()
    }
    val useConsolidatedWildCardPath = if (omsCommandArgs.consolidatedWildCardPaths) {
      true
    } else {
      omsConfig.consolidateWildcardPath
    }
    // Streaming Ingest the Raw Actions for configured Delta tables
    streamingUpdateRawDeltaActionsToOMS(useConsolidatedWildCardPath)
  }
}
