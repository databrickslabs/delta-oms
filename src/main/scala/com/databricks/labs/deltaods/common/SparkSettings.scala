package com.databricks.labs.deltaods.common

import org.apache.spark.sql.SparkSession

trait SparkSettings extends Serializable with ConfigurationSettings {
  protected val sparkSession: SparkSession = environment match {
    case InBuilt => SparkSession.builder()
      .master("local")
      .config("spark.driver.host", "localhost")
      .appName("ODS_INBUILT").getOrCreate()
    case Local => SparkSession.builder()
      .master("local[*]")
      .config("spark.driver.host", "localhost")
      .config("spark.sql.warehouse.dir", odsConfig.baseLocation)
      .appName("ODS_LOCAL")
      .enableHiveSupport()
      .getOrCreate()
    case _ => SparkSession.builder().appName("ODS").getOrCreate()
  }

  def spark = SparkSession.active

}
