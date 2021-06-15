package io.delta.oms.model;
public  class PathConfig implements scala.Product, scala.Serializable {
  static public abstract  R apply (T1 v1, T2 v2, T3 v3, T4 v4, T5 v5, T6 v6, T7 v7, T8 v8, T9 v9)  ;
  static public  java.lang.String toString ()  { throw new RuntimeException(); }
  public  java.lang.String path ()  { throw new RuntimeException(); }
  public  java.lang.String puid ()  { throw new RuntimeException(); }
  public  java.lang.String wildCardPath ()  { throw new RuntimeException(); }
  public  java.lang.String wuid ()  { throw new RuntimeException(); }
  public  boolean automated ()  { throw new RuntimeException(); }
  public  scala.Option<java.lang.String> qualifiedName ()  { throw new RuntimeException(); }
  public  long commit_version ()  { throw new RuntimeException(); }
  public  boolean skipProcessing ()  { throw new RuntimeException(); }
  public  java.time.Instant update_ts ()  { throw new RuntimeException(); }
  // not preceding
  public   PathConfig (java.lang.String path, java.lang.String puid, java.lang.String wildCardPath, java.lang.String wuid, boolean automated, scala.Option<java.lang.String> qualifiedName, long commit_version, boolean skipProcessing, java.time.Instant update_ts)  { throw new RuntimeException(); }
  public  org.apache.spark.sql.delta.DeltaLog getDeltaLog (org.apache.spark.sql.SparkSession spark)  { throw new RuntimeException(); }
}
