package com.databricks.labs.deltaoms.model

import java.time.Instant

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.{DeltaLog}
import org.apache.spark.sql.delta.actions.CommitInfo
import org.apache.spark.sql.types.StructType

case class PathConfig(path: String,
                      puid: String,
                      wildCardPath: String,
                      wuid: String,
                      automated: Boolean = true,
                      qualifiedName: Option[String] = None,
                      commit_version: Long,
                      skipProcessing: Boolean = false,
                      update_ts: Instant = Instant.now()) {
  def getDeltaLog(spark: SparkSession): DeltaLog = {
    DeltaLog.forTable(spark, path)
  }
}

case class AddFileInfo(path: String, size: Long, numRecords: Long)
case class RemoveFileInfo(path: String)
case class PathSnapshot(puid: String,commit_version: Long, inputFiles: Seq[AddFileInfo])

case class DeltaTableHistory(tableConfig: PathConfig,
                             history: Seq[CommitInfo] = Seq.empty[CommitInfo])

case class TableDefinition(
                            tableName: String,
                            databaseName: String = "default",
                            schema: StructType,
                            path: String,
                            comment: Option[String] = None,
                            properties: Map[String, String] = Map.empty[String, String],
                            partitionColumnNames: Seq[String] = Seq.empty[String],
                            version: Long = 0){
  assert(path.nonEmpty & tableName.nonEmpty, "Table Name and Path is required")
}

case class DatabaseDefinition(databaseName: String,
                              location: Option[String],
                              comment: Option[String] = None,
                              properties: Map[String, String] = Map.empty){
  assert(databaseName.nonEmpty, "Database Name is required")
}

case class OMSDeltaCommitInfo(puid: String, path: String, qualifiedName: Option[String],
                              updateTs: Instant, commitInfo: Seq[CommitInfo])