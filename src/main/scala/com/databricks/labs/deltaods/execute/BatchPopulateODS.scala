package com.databricks.labs.deltaods.execute

import com.databricks.labs.deltaods.init.InitODS
import org.apache.spark.sql.internal.StaticSQLConf.WAREHOUSE_PATH

object BatchPopulateODS extends ODSRunner with InitODS {

  def main(args: Array[String]): Unit = {
    println(s"""WAREHOUSE !!!! ==> ${spark.conf.get(WAREHOUSE_PATH.key)}""")
    spark.sql("show databases").show()
    logInfo(s"Starting ODS with Configuration : $odsConfig")
    //Create the ODS Database and Table Structures , if needed
    initializeODS(odsConfig)
    spark.sql("show databases").show()
    val testTables = List(("overwatch_etl", "audit_log_bronze"), ("overwatch_etl", "audit_log_bronze1"))
    //println(fetchLastUpdatedVersions(testTables))
  }
}
