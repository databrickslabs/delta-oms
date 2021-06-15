package io.delta.oms.model;
public  class TableDefinition implements scala.Product, scala.Serializable {
  static public abstract  R apply (T1 v1, T2 v2, T3 v3, T4 v4, T5 v5, T6 v6, T7 v7, T8 v8)  ;
  static public  java.lang.String toString ()  { throw new RuntimeException(); }
  public  java.lang.String tableName ()  { throw new RuntimeException(); }
  public  java.lang.String databaseName ()  { throw new RuntimeException(); }
  public  org.apache.spark.sql.types.StructType schema ()  { throw new RuntimeException(); }
  public  java.lang.String path ()  { throw new RuntimeException(); }
  public  scala.Option<java.lang.String> comment ()  { throw new RuntimeException(); }
  public  scala.collection.immutable.Map<java.lang.String, java.lang.String> properties ()  { throw new RuntimeException(); }
  public  scala.collection.Seq<java.lang.String> partitionColumnNames ()  { throw new RuntimeException(); }
  public  long version ()  { throw new RuntimeException(); }
  // not preceding
  public   TableDefinition (java.lang.String tableName, java.lang.String databaseName, org.apache.spark.sql.types.StructType schema, java.lang.String path, scala.Option<java.lang.String> comment, scala.collection.immutable.Map<java.lang.String, java.lang.String> properties, scala.collection.Seq<java.lang.String> partitionColumnNames, long version)  { throw new RuntimeException(); }
}
