/*
 * Copyright (2021) Databricks, Inc.
 *
 * Delta Operational Metrics Store(DeltaOMS)
 *
 * Copyright 2021 Databricks, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.databricks.labs.deltaoms.init

import com.databricks.labs.deltaoms.common.BatchOMSRunner

object ConfigurePathsFromMetastore extends BatchOMSRunner {

  def main(args: Array[String]): Unit = {
    spark.conf.set("spark.databricks.labs.deltaoms.class", value = getClass.getCanonicalName)
    val consolidatedOMSConfig = consolidateAndValidateOMSConfig(args, omsConfig)
    logInfo(s"Starting Delta table path configuration update for OMS with Configuration : " +
      s"$consolidatedOMSConfig")
    // Create the OMS Database and Path Config Table Structures , if needed
    if(!consolidatedOMSConfig.skipInitializeOMS) {
      initializeOMS(consolidatedOMSConfig)
    }

    // Fetch the latest metastore delta tables and update the Path in the config table
    updateOMSPathConfigFromMetaStore(consolidatedOMSConfig)
  }
}
