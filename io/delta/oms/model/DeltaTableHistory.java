package io.delta.oms.model;
public  class DeltaTableHistory implements scala.Product, scala.Serializable {
  static public abstract  R apply (T1 v1, T2 v2)  ;
  static public  java.lang.String toString ()  { throw new RuntimeException(); }
  public  io.delta.oms.model.PathConfig tableConfig ()  { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.delta.actions.CommitInfo> history ()  { throw new RuntimeException(); }
  // not preceding
  public   DeltaTableHistory (io.delta.oms.model.PathConfig tableConfig, scala.collection.Seq<org.apache.spark.sql.delta.actions.CommitInfo> history)  { throw new RuntimeException(); }
}
