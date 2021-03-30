package com.databricks.labs.deltaoms.ingest

import com.databricks.labs.deltaoms.common.{OMSInitializer, OMSRunner}

object BatchPopulateOMS extends OMSRunner with OMSInitializer {

  def main(args: Array[String]): Unit = {
    //println(s"""WAREHOUSE !!!! ==> ${spark.conf.get(WAREHOUSE_PATH.key)}""")
    logInfo(s"Starting OMS with Configuration : $omsConfig")
    //Create the OMS Database and Table Structures , if needed
    initializeOMS(omsConfig)
    // Fetch the latest Commit history from the Delta Tables in the metastore
    updateRawCommitHistoryToOMS()
  }
}
