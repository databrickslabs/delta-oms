package com.databricks.labs.deltaoms.configuration

case class OMSConfig(baseLocation: String,
                     dbName: String,
                     rawActionTable: String,
                     tableConfig: String,
                     pathConfigTable: String,
                     processedHistoryTable: String,
                     commitInfoSnapshotTable: String,
                     actionSnapshotTable: String,
                     useWildcardPath: Boolean = true,
                     truncatePathConfig: Boolean = false,
                     srcDatabases: Option[String] = None,
                     tablePattern: Option[String] = None,
                     versionFetchSize: Option[Long] = None,
                     checkpointBase: Option[String] = None,
                     checkpointSuffix: Option[String] = None,
                     triggerInterval: Option[String] = None){
  if(versionFetchSize.nonEmpty && versionFetchSize.get < 0){
    throw new IllegalArgumentException("Version Fetch Size should be non negative")
  }
}

case class OMSDatabase(name: String, path: String)



