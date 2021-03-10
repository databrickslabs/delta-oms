package com.databricks.labs.deltaods.ingest

import com.databricks.labs.deltaods.common.{ODSInitializer, ODSRunner}

object BatchPopulateODS extends ODSRunner with ODSInitializer {

  def main(args: Array[String]): Unit = {
    //println(s"""WAREHOUSE !!!! ==> ${spark.conf.get(WAREHOUSE_PATH.key)}""")
    logInfo(s"Starting ODS with Configuration : $odsConfig")
    //Create the ODS Database and Table Structures , if needed
    initializeODS(odsConfig)
    // Fetch the latest Commit history from the Delta Tables in the metastore
    updateRawCommitHistoryToODS()
  }
}
