package io.delta.oms.model;
public  class AddFileInfo implements scala.Product, scala.Serializable {
  static public abstract  R apply (T1 v1, T2 v2, T3 v3)  ;
  static public  java.lang.String toString ()  { throw new RuntimeException(); }
  public  java.lang.String path ()  { throw new RuntimeException(); }
  public  long size ()  { throw new RuntimeException(); }
  public  long numRecords ()  { throw new RuntimeException(); }
  // not preceding
  public   AddFileInfo (java.lang.String path, long size, long numRecords)  { throw new RuntimeException(); }
}
