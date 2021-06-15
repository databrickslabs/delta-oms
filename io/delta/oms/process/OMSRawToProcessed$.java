package io.delta.oms.process;
public  class OMSRawToProcessed$ implements io.delta.oms.common.OMSRunner, io.delta.oms.common.OMSInitializer {
  /**
   * Static reference to the singleton instance of this Scala object.
   */
  public static final OMSRawToProcessed$ MODULE$ = null;
  public  org.apache.spark.sql.SparkSession.implicits$ implicits ()  { throw new RuntimeException(); }
  public final  org.apache.spark.sql.types.StructType rawCommit ()  { throw new RuntimeException(); }
  public final  org.apache.spark.sql.types.StructType rawAction ()  { throw new RuntimeException(); }
  public final  org.apache.spark.sql.types.StructType pathConfig ()  { throw new RuntimeException(); }
  public final  org.apache.spark.sql.types.StructType tableConfig ()  { throw new RuntimeException(); }
  public final  org.apache.spark.sql.types.StructType processedHistory ()  { throw new RuntimeException(); }
  protected  org.apache.spark.sql.SparkSession sparkSession ()  { throw new RuntimeException(); }
  public   OMSRawToProcessed$ ()  { throw new RuntimeException(); }
  public  void main (java.lang.String[] args)  { throw new RuntimeException(); }
  public  org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> computeActionSnapshotFromRawActions (org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> rawActions, boolean snapshotExists)  { throw new RuntimeException(); }
  public  org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> prepareAddRemoveActionsFromRawActions (org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> rawActions)  { throw new RuntimeException(); }
  public  org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> computeCumulativeFilesFromAddRemoveActions (org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> addRemoveActions)  { throw new RuntimeException(); }
  public  org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> deriveActionSnapshotFromCumulativeActions (org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> cumulativeAddRemoveFiles)  { throw new RuntimeException(); }
}
