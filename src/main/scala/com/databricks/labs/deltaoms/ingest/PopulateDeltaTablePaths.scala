package com.databricks.labs.deltaoms.ingest

import com.databricks.labs.deltaoms.common.{OMSInitializer, OMSRunner}

object PopulateDeltaTablePaths extends OMSRunner with OMSInitializer {

  def main(args: Array[String]): Unit = {
    logInfo(s"Starting Delta table path configuration update for OMS with Configuration : $omsConfig")
    //Create the OMS Database and Path Config Table Structures , if needed
    initializeOMS(omsConfig)
    //Update the OMS Path Config from Table Config
    updateOMSPathConfigFromTableConfig()
  }
}
