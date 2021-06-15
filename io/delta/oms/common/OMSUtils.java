package io.delta.oms.common;
public  interface OMSUtils extends scala.Serializable, org.apache.spark.internal.Logging, io.delta.oms.configuration.ConfigurationSettings, io.delta.oms.common.OMSchemas {
  public  java.lang.String omsDBPath ()  ;
  public  java.lang.String rawActionsTablePath ()  ;
  public  java.lang.String pathConfigTablePath ()  ;
  public  java.lang.String tableConfigPath ()  ;
  public  java.lang.String processedHistoryTablePath ()  ;
  public  java.lang.String commitSnapshotTablePath ()  ;
  public  java.lang.String commitSnapshotTableName ()  ;
  public  java.lang.String actionSnapshotTablePath ()  ;
  public  java.lang.String actionSnapshotTableName ()  ;
  public  scala.collection.Seq<java.lang.String> puidCommitDatePartitions ()  ;
  public  io.delta.oms.model.TableDefinition pathConfigTableDefinition (io.delta.oms.configuration.OMSConfig omsConfig)  ;
  public  io.delta.oms.model.TableDefinition tableConfigDefinition (io.delta.oms.configuration.OMSConfig omsConfig)  ;
  public  io.delta.oms.model.TableDefinition rawActionsTableDefinition (io.delta.oms.configuration.OMSConfig omsConfig)  ;
  public  io.delta.oms.model.TableDefinition processedHistoryTableDefinition (io.delta.oms.configuration.OMSConfig omsConfig)  ;
  public  io.delta.oms.model.DatabaseDefinition omsDatabaseDefinition (io.delta.oms.configuration.OMSConfig omsConfig)  ;
}
