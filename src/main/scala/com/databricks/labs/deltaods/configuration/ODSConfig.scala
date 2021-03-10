package com.databricks.labs.deltaods.configuration

case class ODSConfig(baseLocation: String,
                     dbName: String,
                     rawCommitTable: String,
                     rawActionTable: String,
                     pathConfigTable: String,
                     srcDatabases: Option[String] = None,
                     tablePattern: Option[String] = None,
                     versionFetchSize: Option[Long] = None,
                     odsCheckpointBase: Option[String] = None){
  if(versionFetchSize.nonEmpty && versionFetchSize.get < 0){
    throw new IllegalArgumentException("Version Fetch Size should be non negative")
  }
}

case class ODSDatabase(name: String, path: String)



