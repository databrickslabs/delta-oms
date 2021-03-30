package com.databricks.labs.deltaoms.configuration

import org.apache.spark.sql.SparkSession

trait SparkSettings extends Serializable with ConfigurationSettings {
  protected val sparkSession: SparkSession = environment match {
    case InBuilt => SparkSession.builder()
      .master("local")
      .config("spark.driver.host", "localhost")
      .appName("OMS_INBUILT").getOrCreate()
    case Local => SparkSession.builder()
      .master("local[*]")
      .config("spark.driver.host", "localhost")
      .config("spark.sql.warehouse.dir", omsConfig.baseLocation)
      .appName("OMS_LOCAL")
      .enableHiveSupport()
      .getOrCreate()
    case _ => SparkSession.builder().appName("OMS").getOrCreate()
  }

  def spark = SparkSession.active

}
