package io.delta.oms.common;
public  interface OMSInitializer extends scala.Serializable, org.apache.spark.internal.Logging {
  public  void cleanupOMS (io.delta.oms.configuration.OMSConfig config)  ;
  public  void createOMSDB (io.delta.oms.configuration.OMSConfig config)  ;
  public  void createOMSTables (io.delta.oms.configuration.OMSConfig config)  ;
  public  void createPathConfigTables (io.delta.oms.configuration.OMSConfig config)  ;
  public  void initializeOMS (io.delta.oms.configuration.OMSConfig config, boolean dropAndRecreate)  ;
  public  void initializeOMSPathConfig (io.delta.oms.configuration.OMSConfig config, boolean dropAndRecreate)  ;
}
