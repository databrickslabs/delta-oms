package com.databricks.labs.deltaoms.ingest

import com.databricks.labs.deltaoms.common.{OMSInitializer, OMSRunner}

object InitializeOMSTables extends OMSRunner with OMSInitializer {

  def main(args: Array[String]): Unit = {
    logInfo(s"Initializing Delta OMS Database and tables with Configuration : $omsConfig")
    //Create the OMS Database and Table Structures , if needed
    initializeOMS(omsConfig, dropAndRecreate=true)
  }
}
