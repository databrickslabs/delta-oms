package com.databricks.labs.deltaods.configuration

case class ODSConfig(baseLocation: String, dbName: String, latestVersionTable: String, rawCommitTable: String)

case class ODSDatabase(name: String, path: String)



