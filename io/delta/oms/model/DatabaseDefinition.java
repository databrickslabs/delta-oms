package io.delta.oms.model;
public  class DatabaseDefinition implements scala.Product, scala.Serializable {
  static public abstract  R apply (T1 v1, T2 v2, T3 v3, T4 v4)  ;
  static public  java.lang.String toString ()  { throw new RuntimeException(); }
  public  java.lang.String databaseName ()  { throw new RuntimeException(); }
  public  scala.Option<java.lang.String> location ()  { throw new RuntimeException(); }
  public  scala.Option<java.lang.String> comment ()  { throw new RuntimeException(); }
  public  scala.collection.immutable.Map<java.lang.String, java.lang.String> properties ()  { throw new RuntimeException(); }
  // not preceding
  public   DatabaseDefinition (java.lang.String databaseName, scala.Option<java.lang.String> location, scala.Option<java.lang.String> comment, scala.collection.immutable.Map<java.lang.String, java.lang.String> properties)  { throw new RuntimeException(); }
}
