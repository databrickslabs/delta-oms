package io.delta.oms.configuration;
public  interface ConfigurationSettings extends scala.Serializable, org.apache.spark.internal.Logging {
  public  pureconfig.ConfigObjectSource configSource ()  ;
  public  io.delta.oms.configuration.Environment environment ()  ;
  public  java.lang.String environmentConfigFile ()  ;
  public  java.lang.String fetchConfigFileContent (java.lang.String fullFilePath)  ;
  public  io.delta.oms.configuration.OMSConfig omsConfig ()  ;
}
