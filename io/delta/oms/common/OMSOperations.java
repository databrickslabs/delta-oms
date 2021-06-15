package io.delta.oms.common;
public  interface OMSOperations extends scala.Serializable, io.delta.oms.configuration.SparkSettings, org.apache.spark.internal.Logging, io.delta.oms.common.OMSchemas {
  public  org.apache.spark.sql.Dataset<io.delta.oms.model.PathConfig> fetchPathConfigForProcessing ()  ;
  public  java.lang.String[] fetchPathForStreamProcessing (boolean useWildCardPath)  ;
  public  scala.Option<org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>> fetchStreamingDeltaLogForPath (java.lang.String path, boolean useAutoloader)  ;
  public  java.lang.String[] fetchTableConfigForProcessing ()  ;
  public  scala.Option<org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>> getDeltaLogs (org.apache.spark.sql.types.StructType schema, java.lang.String path)  ;
  public  org.apache.spark.sql.SparkSession.implicits$ implicits ()  ;
  public  org.apache.spark.sql.streaming.StreamingQuery streamingUpdateRawDeltaActionsToOMS (boolean useWildCardPath)  ;
  public  org.apache.spark.sql.Dataset<io.delta.oms.model.PathConfig> tablePathToPathConfig (org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> tablePaths)  ;
  public  void updateOMSPathConfigFromList (scala.collection.Seq<java.lang.String> locations, boolean truncate)  ;
  public  void updateOMSPathConfigFromMetaStore (boolean truncate)  ;
  public  void updateOMSPathConfigFromTableConfig ()  ;
  public  void updatePathConfigToOMS (org.apache.spark.sql.Dataset<io.delta.oms.model.PathConfig> pathConfigs, boolean truncate)  ;
  public  void updatePathConfigWithLatestVersions (org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> lastUpdatedDF)  ;
}
