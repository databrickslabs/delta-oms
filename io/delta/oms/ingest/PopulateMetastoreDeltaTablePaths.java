package io.delta.oms.ingest;
public  class PopulateMetastoreDeltaTablePaths {
  static public  org.apache.spark.sql.SparkSession.implicits$ implicits ()  { throw new RuntimeException(); }
  static public final  org.apache.spark.sql.types.StructType rawCommit ()  { throw new RuntimeException(); }
  static public final  org.apache.spark.sql.types.StructType rawAction ()  { throw new RuntimeException(); }
  static public final  org.apache.spark.sql.types.StructType pathConfig ()  { throw new RuntimeException(); }
  static public final  org.apache.spark.sql.types.StructType tableConfig ()  { throw new RuntimeException(); }
  static public final  org.apache.spark.sql.types.StructType processedHistory ()  { throw new RuntimeException(); }
  static protected  org.apache.spark.sql.SparkSession sparkSession ()  { throw new RuntimeException(); }
  static public  void main (java.lang.String[] args)  { throw new RuntimeException(); }
  static public  org.slf4j.Logger org$apache$spark$internal$Logging$$log_ ()  { throw new RuntimeException(); }
  static public  void org$apache$spark$internal$Logging$$log__$eq (org.slf4j.Logger x$1)  { throw new RuntimeException(); }
  static protected  java.lang.String logName ()  { throw new RuntimeException(); }
  static protected  org.slf4j.Logger log ()  { throw new RuntimeException(); }
  static protected  void logInfo (scala.Function0<java.lang.String> msg)  { throw new RuntimeException(); }
  static protected  void logDebug (scala.Function0<java.lang.String> msg)  { throw new RuntimeException(); }
  static protected  void logTrace (scala.Function0<java.lang.String> msg)  { throw new RuntimeException(); }
  static protected  void logWarning (scala.Function0<java.lang.String> msg)  { throw new RuntimeException(); }
  static protected  void logError (scala.Function0<java.lang.String> msg)  { throw new RuntimeException(); }
  static protected  void logInfo (scala.Function0<java.lang.String> msg, java.lang.Throwable throwable)  { throw new RuntimeException(); }
  static protected  void logDebug (scala.Function0<java.lang.String> msg, java.lang.Throwable throwable)  { throw new RuntimeException(); }
  static protected  void logTrace (scala.Function0<java.lang.String> msg, java.lang.Throwable throwable)  { throw new RuntimeException(); }
  static protected  void logWarning (scala.Function0<java.lang.String> msg, java.lang.Throwable throwable)  { throw new RuntimeException(); }
  static protected  void logError (scala.Function0<java.lang.String> msg, java.lang.Throwable throwable)  { throw new RuntimeException(); }
  static protected  boolean isTraceEnabled ()  { throw new RuntimeException(); }
  static protected  void initializeLogIfNecessary (boolean isInterpreter)  { throw new RuntimeException(); }
  static protected  boolean initializeLogIfNecessary (boolean isInterpreter, boolean silent)  { throw new RuntimeException(); }
  static protected  boolean initializeLogIfNecessary$default$2 ()  { throw new RuntimeException(); }
  static   void initializeForcefully (boolean isInterpreter, boolean silent)  { throw new RuntimeException(); }
  static public  io.delta.oms.configuration.OMSConfig omsConfig ()  { throw new RuntimeException(); }
  static public  pureconfig.ConfigObjectSource configSource ()  { throw new RuntimeException(); }
  static public  io.delta.oms.configuration.Environment environment ()  { throw new RuntimeException(); }
  static public  java.lang.String environmentConfigFile ()  { throw new RuntimeException(); }
  static public  java.lang.String fetchConfigFileContent (java.lang.String fullFilePath)  { throw new RuntimeException(); }
  static public  org.apache.spark.sql.SparkSession spark ()  { throw new RuntimeException(); }
  static protected abstract  void io$delta$oms$configuration$SparkSettings$_setter_$sparkSession_$eq (org.apache.spark.sql.SparkSession x$1)  ;
  static public final  java.lang.String PATH ()  { throw new RuntimeException(); }
  static public final  java.lang.String PUID ()  { throw new RuntimeException(); }
  static public final  java.lang.String WUID ()  { throw new RuntimeException(); }
  static public final  java.lang.String QUALIFIED_NAME ()  { throw new RuntimeException(); }
  static public final  java.lang.String UPDATE_TS ()  { throw new RuntimeException(); }
  static public final  java.lang.String COMMIT_DATE ()  { throw new RuntimeException(); }
  static public final  java.lang.String COMMIT_TS ()  { throw new RuntimeException(); }
  static public final  java.lang.String COMMIT_VERSION ()  { throw new RuntimeException(); }
  static public final  java.lang.String FILE_NAME ()  { throw new RuntimeException(); }
  static protected abstract  void io$delta$oms$common$OMSchemas$_setter_$rawCommit_$eq (org.apache.spark.sql.types.StructType x$1)  ;
  static protected abstract  void io$delta$oms$common$OMSchemas$_setter_$rawAction_$eq (org.apache.spark.sql.types.StructType x$1)  ;
  static protected abstract  void io$delta$oms$common$OMSchemas$_setter_$pathConfig_$eq (org.apache.spark.sql.types.StructType x$1)  ;
  static protected abstract  void io$delta$oms$common$OMSchemas$_setter_$tableConfig_$eq (org.apache.spark.sql.types.StructType x$1)  ;
  static protected abstract  void io$delta$oms$common$OMSchemas$_setter_$processedHistory_$eq (org.apache.spark.sql.types.StructType x$1)  ;
  static public  void updatePathConfigWithLatestVersions (org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> lastUpdatedDF)  { throw new RuntimeException(); }
  static public  void updateOMSPathConfigFromTableConfig ()  { throw new RuntimeException(); }
  static public  java.lang.String[] fetchTableConfigForProcessing ()  { throw new RuntimeException(); }
  static public  void updateOMSPathConfigFromList (scala.collection.Seq<java.lang.String> locations, boolean truncate)  { throw new RuntimeException(); }
  static public  boolean updateOMSPathConfigFromList$default$2 ()  { throw new RuntimeException(); }
  static public  void updateOMSPathConfigFromMetaStore (boolean truncate)  { throw new RuntimeException(); }
  static public  boolean updateOMSPathConfigFromMetaStore$default$1 ()  { throw new RuntimeException(); }
  static public  org.apache.spark.sql.Dataset<io.delta.oms.model.PathConfig> tablePathToPathConfig (org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> tablePaths)  { throw new RuntimeException(); }
  static public  void updatePathConfigToOMS (org.apache.spark.sql.Dataset<io.delta.oms.model.PathConfig> pathConfigs, boolean truncate)  { throw new RuntimeException(); }
  static public  boolean updatePathConfigToOMS$default$2 ()  { throw new RuntimeException(); }
  static public  org.apache.spark.sql.streaming.StreamingQuery streamingUpdateRawDeltaActionsToOMS (boolean useWildCardPath)  { throw new RuntimeException(); }
  static public  java.lang.String[] fetchPathForStreamProcessing (boolean useWildCardPath)  { throw new RuntimeException(); }
  static public  boolean fetchPathForStreamProcessing$default$1 ()  { throw new RuntimeException(); }
  static public  org.apache.spark.sql.Dataset<io.delta.oms.model.PathConfig> fetchPathConfigForProcessing ()  { throw new RuntimeException(); }
  static public  scala.Option<org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>> fetchStreamingDeltaLogForPath (java.lang.String path, boolean useAutoloader)  { throw new RuntimeException(); }
  static public  boolean fetchStreamingDeltaLogForPath$default$2 ()  { throw new RuntimeException(); }
  static public  scala.Option<org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>> getDeltaLogs (org.apache.spark.sql.types.StructType schema, java.lang.String path)  { throw new RuntimeException(); }
  static protected abstract  void io$delta$oms$common$OMSOperations$_setter_$implicits_$eq (org.apache.spark.sql.SparkSession.implicits$ x$1)  ;
  static public  void initializeOMSPathConfig (io.delta.oms.configuration.OMSConfig config, boolean dropAndRecreate)  { throw new RuntimeException(); }
  static public  boolean initializeOMSPathConfig$default$2 ()  { throw new RuntimeException(); }
  static public  void initializeOMS (io.delta.oms.configuration.OMSConfig config, boolean dropAndRecreate)  { throw new RuntimeException(); }
  static public  boolean initializeOMS$default$2 ()  { throw new RuntimeException(); }
  static public  void createOMSDB (io.delta.oms.configuration.OMSConfig config)  { throw new RuntimeException(); }
  static public  void createOMSTables (io.delta.oms.configuration.OMSConfig config)  { throw new RuntimeException(); }
  static public  void createPathConfigTables (io.delta.oms.configuration.OMSConfig config)  { throw new RuntimeException(); }
  static public  void cleanupOMS (io.delta.oms.configuration.OMSConfig config)  { throw new RuntimeException(); }
}
