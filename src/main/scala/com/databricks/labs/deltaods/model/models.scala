package com.databricks.labs.deltaods.model

import java.time.Instant

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.delta.DeltaTableIdentifier
import org.apache.spark.sql.delta.actions.CommitInfo
import org.apache.spark.sql.types.StructType

case class PathConfig(path: String,
                      uid: String,
                      automated: Boolean = true,
                      skipProcessing: Boolean = false,
                      updateTs: Instant = Instant.now())

case class TablePathWithVersion(qualifiedName: String , path: String,
                                version: Long,refreshTimestamp: Instant)
case class DeltaTableIdentifierWithLatestVersion(table: DeltaTableIdentifier,
                                                 version: Long = 0,
                                                 refreshTimestamp: Instant = Instant.now()){
  def getQualifiedName = table.unquotedString
  def getTablePathWithVersion(spark: SparkSession) = {
    TablePathWithVersion(getQualifiedName, table.getPath(spark).toString,
      version,refreshTimestamp)
  }
  def getPathString(spark: SparkSession) = {
    table.getPath(spark).toString
  }
  def getPathConfig(spark: SparkSession) = {
    PathConfig(getPathString(spark),java.util.UUID.randomUUID().toString)
  }
}
object DeltaTableIdentifierWithLatestVersion {
  def apply(spark: SparkSession, identifier: TableIdentifier): Option[DeltaTableIdentifierWithLatestVersion] = {
    val dtId = DeltaTableIdentifier(spark, identifier)
    if (dtId.nonEmpty) {
      Some(DeltaTableIdentifierWithLatestVersion(dtId.get))
    } else {
      None
    }
  }
}

case class DeltaTableHistory(table: DeltaTableIdentifierWithLatestVersion,history: Seq[CommitInfo])

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

case class ODSCommitInfo(path: String, qualifiedName: String,
                         commitDate: java.time.LocalDate, commitInfo: Seq[CommitInfo])