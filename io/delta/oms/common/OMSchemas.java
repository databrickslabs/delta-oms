package io.delta.oms.common;
public  interface OMSchemas {
  public  java.lang.String COMMIT_DATE ()  ;
  public  java.lang.String COMMIT_TS ()  ;
  public  java.lang.String COMMIT_VERSION ()  ;
  public  java.lang.String FILE_NAME ()  ;
  public  java.lang.String PATH ()  ;
  public  java.lang.String PUID ()  ;
  public  java.lang.String QUALIFIED_NAME ()  ;
  public  java.lang.String UPDATE_TS ()  ;
  public  java.lang.String WUID ()  ;
  public  org.apache.spark.sql.types.StructType pathConfig ()  ;
  public  org.apache.spark.sql.types.StructType processedHistory ()  ;
  public  org.apache.spark.sql.types.StructType rawAction ()  ;
  public  org.apache.spark.sql.types.StructType rawCommit ()  ;
  public  org.apache.spark.sql.types.StructType tableConfig ()  ;
}
