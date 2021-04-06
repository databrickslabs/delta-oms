package com.databricks.labs.deltaoms.ingest

import com.databricks.labs.deltaoms.common.{OMSInitializer, OMSRunner}

object StreamPopulateOMS extends OMSRunner with OMSInitializer {

  def main(args: Array[String]): Unit = {
    logInfo(s"Starting Streaming OMS with Configuration : $omsConfig")
    //Create the OMS Database and Table Structures , if needed
    initializeOMS(omsConfig)
    //Update the OMS Path Config from Table Config
    updateOMSPathConfigFromTableConfig()
    // Streaming Ingest the Raw Actions for configured Delta tables
    streamingUpdateRawDeltaActionsToOMS()
  }
}
