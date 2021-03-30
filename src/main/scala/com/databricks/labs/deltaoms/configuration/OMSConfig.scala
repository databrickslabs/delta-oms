package com.databricks.labs.deltaoms.configuration

case class OMSConfig(baseLocation: String,
                     dbName: String,
                     rawCommitTable: String,
                     rawActionTable: String,
                     pathConfigTable: String,
                     pathSnapshotTable: String,
                     srcDatabases: Option[String] = None,
                     tablePattern: Option[String] = None,
                     versionFetchSize: Option[Long] = None,
                     omsCheckpointBase: Option[String] = None){
  if(versionFetchSize.nonEmpty && versionFetchSize.get < 0){
    throw new IllegalArgumentException("Version Fetch Size should be non negative")
  }
}

case class OMSDatabase(name: String, path: String)



