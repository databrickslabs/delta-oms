package com.databricks.labs.deltaods.model

import java.time.Instant

import org.apache.spark.sql.types.StructType

case class TableIdentifierWithLatestVersion(tableName: String, databaseName: String = "default",
                                            version: Long = 0, refreshTimestamp: Instant = Instant.now())

case class TableHistory(tableName: String, databaseName: String, history: org.apache.spark.sql.DataFrame)

case class TableDefinition(
                            tableName: String,
                            databaseName: String = "default",
                            schema: StructType,
                            path: String,
                            comment: Option[String] = None,
                            properties: Map[String, String] = Map.empty[String, String],
                            partitionColumnNames: Seq[String] = Seq.empty[String])

case class DatabaseDefinition(databaseName: String,
                              location: Option[String],
                              comment: Option[String] = None,
                              properties: Map[String, String] = Map.empty)