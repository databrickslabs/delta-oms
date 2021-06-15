package io.delta.oms.configuration;
public  interface SparkSettings extends scala.Serializable, io.delta.oms.configuration.ConfigurationSettings {
  public  org.apache.spark.sql.SparkSession spark ()  ;
  public  org.apache.spark.sql.SparkSession sparkSession ()  ;
}
