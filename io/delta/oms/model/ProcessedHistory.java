package io.delta.oms.model;
public  class ProcessedHistory implements scala.Product, scala.Serializable {
  static public abstract  R apply (T1 v1, T2 v2, T3 v3)  ;
  static public  java.lang.String toString ()  { throw new RuntimeException(); }
  public  java.lang.String tableName ()  { throw new RuntimeException(); }
  public  long lastVersion ()  { throw new RuntimeException(); }
  public  java.time.Instant update_ts ()  { throw new RuntimeException(); }
  // not preceding
  public   ProcessedHistory (java.lang.String tableName, long lastVersion, java.time.Instant update_ts)  { throw new RuntimeException(); }
}
