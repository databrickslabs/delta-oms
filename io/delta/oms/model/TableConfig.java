package io.delta.oms.model;
public  class TableConfig implements scala.Product, scala.Serializable {
  static public abstract  R apply (T1 v1, T2 v2)  ;
  static public  java.lang.String toString ()  { throw new RuntimeException(); }
  public  java.lang.String path ()  { throw new RuntimeException(); }
  public  boolean skipProcessing ()  { throw new RuntimeException(); }
  // not preceding
  public   TableConfig (java.lang.String path, boolean skipProcessing)  { throw new RuntimeException(); }
}
