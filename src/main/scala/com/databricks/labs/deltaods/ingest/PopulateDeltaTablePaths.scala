package com.databricks.labs.deltaods.ingest

import com.databricks.labs.deltaods.common.{ODSInitializer, ODSRunner}

object PopulateDeltaTablePaths extends ODSRunner with ODSInitializer {

  def main(args: Array[String]): Unit = {
    logInfo(s"Starting Delta table path configuration update for ODS with Configuration : $odsConfig")
    //Create the ODS Database and Path Config Table Structures , if needed
    initializeODSPathConfig(odsConfig)
    // Fetch the latest metastore delta tables and update the Path in the config table
    updateODSPathConfigFromMetaStore()
  }
}
