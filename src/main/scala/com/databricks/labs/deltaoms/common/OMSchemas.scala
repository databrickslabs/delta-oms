package com.databricks.labs.deltaoms.common

import com.databricks.labs.deltaoms.model.{PathConfig, ProcessedHistory, TableConfig}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.delta.actions.{CommitInfo, SingleAction}
import org.apache.spark.sql.types._

trait OMSchemas {
  final val PATH = "path"
  final val PUID = "puid"
  final val WUID = "wuid"
  final val QUALIFIED_NAME = "qualifiedName"
  final val UPDATE_TS = "update_ts"
  final val COMMIT_DATE = "commit_date"
  final val COMMIT_TS = "commit_ts"
  final val COMMIT_VERSION = "commit_version"
  final val FILE_NAME = "file_name"

  final val rawCommit = ScalaReflection.schemaFor[CommitInfo].dataType.asInstanceOf[StructType]
    .add(StructField(PATH, StringType))
    .add(StructField(PUID, StringType))
    .add(StructField(QUALIFIED_NAME, StringType))
    .add(StructField(UPDATE_TS, TimestampType))
    .add(StructField(COMMIT_DATE, DateType))
  final val rawAction = ScalaReflection.schemaFor[SingleAction].dataType.asInstanceOf[StructType]
    .add(StructField(FILE_NAME, StringType))
    .add(StructField(PATH, StringType))
    .add(StructField(PUID, StringType))
    .add(StructField(COMMIT_VERSION, LongType))
    .add(StructField(UPDATE_TS, TimestampType))
    .add(StructField(COMMIT_TS, TimestampType))
    .add(StructField(COMMIT_DATE, DateType))
  final val pathConfig = ScalaReflection.schemaFor[PathConfig].dataType.asInstanceOf[StructType]
  final val tableConfig = ScalaReflection.schemaFor[TableConfig].dataType.asInstanceOf[StructType]
  final val processedHistory = ScalaReflection.schemaFor[ProcessedHistory].dataType.asInstanceOf[StructType]
}

object OMSSchemas extends OMSchemas
