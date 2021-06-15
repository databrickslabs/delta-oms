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

import io.delta.oms.common.{OMSInitializer, OMSRunner}

object PopulateMetastoreDeltaTablePaths extends OMSRunner with OMSInitializer {

  def main(args: Array[String]): Unit = {
    logInfo(s"Starting Delta table path configuration update for OMS with Configuration : " +
      s"$omsConfig")
    // Create the OMS Database and Path Config Table Structures , if needed
    initializeOMS(omsConfig)
    // Fetch the latest metastore delta tables and update the Path in the config table
    updateOMSPathConfigFromMetaStore()
  }
}
