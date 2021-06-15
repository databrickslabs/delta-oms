package io.delta.oms.utils;
public  interface UtilityOperations extends scala.Serializable, org.apache.spark.internal.Logging {
  public  scala.collection.Seq<org.apache.spark.sql.delta.DeltaTableIdentifier> fetchMetaStoreDeltaTables (scala.Option<java.lang.String> databases, scala.Option<java.lang.String> pattern)  ;
  public  scala.collection.Seq<org.apache.spark.sql.delta.DeltaTableIdentifier> getDeltaTablesFromMetastore (scala.collection.Seq<java.lang.String> databases, java.lang.String pattern)  ;
  public  scala.collection.Seq<scala.Tuple2<scala.Option<java.lang.String>, java.lang.String>> validateDeltaLocation (java.lang.String locationId)  ;
  public  scala.Option<org.apache.spark.sql.delta.DeltaTableIdentifier> tableIdentifierToDeltaTableIdentifier (org.apache.spark.sql.catalyst.TableIdentifier identifier)  ;
  public  scala.collection.Seq<org.apache.hadoop.fs.Path> getDeltaPathFromDelTableIdentifiers (scala.collection.Seq<org.apache.spark.sql.delta.DeltaTableIdentifier> deltaTableIds)  ;
  public  void createDatabaseIfAbsent (io.delta.oms.model.DatabaseDefinition dbDefn)  ;
  public  void createTableIfAbsent (io.delta.oms.model.TableDefinition tableDefn)  ;
  public  boolean deleteDirectory (java.lang.String dirName)  ;
  public  org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> dropDatabase (java.lang.String dbName)  ;
  public  org.apache.spark.sql.expressions.UserDefinedFunction getFileModificationTimeUDF ()  ;
  public  org.apache.spark.sql.expressions.UserDefinedFunction getDeltaWildCardPathUDF ()  ;
}
