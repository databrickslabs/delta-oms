/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.oms.model

import java.time.Instant

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog
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

case class TableConfig(path: String, skipProcessing: Boolean = false)

case class AddFileInfo(path: String, size: Long, numRecords: Long)

case class RemoveFileInfo(path: String)

case class ProcessedHistory(tableName: String, lastVersion: Long,
  update_ts: Instant = Instant.now())

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
  version: Long = 0) {
  assert(path.nonEmpty & tableName.nonEmpty, "Table Name and Path is required")
}

case class DatabaseDefinition(databaseName: String,
  location: Option[String],
  comment: Option[String] = None,
  properties: Map[String, String] = Map.empty) {
  assert(databaseName.nonEmpty, "Database Name is required")
}

case class OMSCommandLineArgs(skipPathConfig: Boolean = false,
  skipInitializeOMS: Boolean = false,
  useWildCardPaths: Boolean = false)
