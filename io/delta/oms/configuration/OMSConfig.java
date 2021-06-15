package io.delta.oms.configuration;
public  class OMSConfig implements scala.Product, scala.Serializable {
  static public abstract  R apply (T1 v1, T2 v2, T3 v3, T4 v4, T5 v5, T6 v6, T7 v7, T8 v8, T9 v9, T10 v10, T11 v11, T12 v12, T13 v13, T14 v14, T15 v15, T16 v16)  ;
  static public  java.lang.String toString ()  { throw new RuntimeException(); }
  public  java.lang.String baseLocation ()  { throw new RuntimeException(); }
  public  java.lang.String dbName ()  { throw new RuntimeException(); }
  public  java.lang.String rawActionTable ()  { throw new RuntimeException(); }
  public  java.lang.String tableConfig ()  { throw new RuntimeException(); }
  public  java.lang.String pathConfigTable ()  { throw new RuntimeException(); }
  public  java.lang.String processedHistoryTable ()  { throw new RuntimeException(); }
  public  java.lang.String commitInfoSnapshotTable ()  { throw new RuntimeException(); }
  public  java.lang.String actionSnapshotTable ()  { throw new RuntimeException(); }
  public  boolean useWildcardPath ()  { throw new RuntimeException(); }
  public  boolean truncatePathConfig ()  { throw new RuntimeException(); }
  public  scala.Option<java.lang.String> srcDatabases ()  { throw new RuntimeException(); }
  public  scala.Option<java.lang.String> tablePattern ()  { throw new RuntimeException(); }
  public  scala.Option<java.lang.Object> versionFetchSize ()  { throw new RuntimeException(); }
  public  scala.Option<java.lang.String> checkpointBase ()  { throw new RuntimeException(); }
  public  scala.Option<java.lang.String> checkpointSuffix ()  { throw new RuntimeException(); }
  public  scala.Option<java.lang.String> triggerInterval ()  { throw new RuntimeException(); }
  // not preceding
  public   OMSConfig (java.lang.String baseLocation, java.lang.String dbName, java.lang.String rawActionTable, java.lang.String tableConfig, java.lang.String pathConfigTable, java.lang.String processedHistoryTable, java.lang.String commitInfoSnapshotTable, java.lang.String actionSnapshotTable, boolean useWildcardPath, boolean truncatePathConfig, scala.Option<java.lang.String> srcDatabases, scala.Option<java.lang.String> tablePattern, scala.Option<java.lang.Object> versionFetchSize, scala.Option<java.lang.String> checkpointBase, scala.Option<java.lang.String> checkpointSuffix, scala.Option<java.lang.String> triggerInterval)  { throw new RuntimeException(); }
}
