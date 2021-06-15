package io.delta.oms.utils;
public  class DataFrameOperations {
  static public  class DataFrameOps<T extends java.lang.Object> extends scala.AnyVal {
    public  org.apache.spark.sql.Dataset<T> logData ()  { throw new RuntimeException(); }
    // not preceding
    public   DataFrameOps (org.apache.spark.sql.Dataset<T> logData)  { throw new RuntimeException(); }
    public  void writeDataToDeltaTable (java.lang.String path, java.lang.String mode, scala.collection.Seq<java.lang.String> partitionColumnNames)  { throw new RuntimeException(); }
  }
  static public  class DataFrameOps$ {
    /**
     * Static reference to the singleton instance of this Scala object.
     */
    public static final DataFrameOps$ MODULE$ = null;
    public   DataFrameOps$ ()  { throw new RuntimeException(); }
  }
}
