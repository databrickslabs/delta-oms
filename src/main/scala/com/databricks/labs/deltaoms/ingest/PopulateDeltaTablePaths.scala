package com.databricks.labs.deltaoms.ingest

import com.databricks.labs.deltaoms.common.{OMSInitializer, OMSRunner}

object PopulateDeltaTablePaths extends OMSRunner with OMSInitializer {

  def main(args: Array[String]): Unit = {
    logInfo(s"Starting Delta table path configuration update for OMS with Configuration : $omsConfig")
    //Create the OMS Database and Path Config Table Structures , if needed
    initializeOMSPathConfig(omsConfig)
    // Fetch the latest metastore delta tables and update the Path in the config table
    updateOMSPathConfigFromMetaStore()
  }
}
