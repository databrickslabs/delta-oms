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

package com.databricks.labs.deltaoms.ingest

import com.databricks.labs.deltaoms.common.BatchOMSRunner

object InitializeOMSTables extends BatchOMSRunner {

  def main(args: Array[String]): Unit = {

    val consolidatedOMSConfig = consolidateAndValidateOMSConfig(args, omsConfig)
    logInfo(s"Initializing Delta OMS Database and tables with Configuration " +
      s": $consolidatedOMSConfig")
    // Create the OMS Database and Table Structures , if needed
    if(!consolidatedOMSConfig.skipInitializeOMS) {
      initializeOMS(consolidatedOMSConfig)
    }
    // Update the OMS Path Config from Table Config
    if(!consolidatedOMSConfig.skipPathConfig) {
      updateOMSPathConfigFromSourceConfig(consolidatedOMSConfig)
    }
    // Create the OMS Database and Table Structures , if needed
    initializeOMS(consolidatedOMSConfig, dropAndRecreate = true)
  }
}
