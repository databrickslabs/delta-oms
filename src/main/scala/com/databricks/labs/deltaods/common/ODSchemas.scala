package com.databricks.labs.deltaods.common

import com.databricks.labs.deltaods.model.PathConfig
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.delta.actions.{CommitInfo, SingleAction}
import org.apache.spark.sql.types._

trait ODSchemas {
  final val rawCommit = ScalaReflection.schemaFor[CommitInfo].dataType.asInstanceOf[StructType]
    .add(StructField("path", StringType))
    .add(StructField("puid", StringType))
    .add(StructField("qualifiedName", StringType))
    .add(StructField("updateTs", TimestampType))
    .add(StructField("commitDate", DateType))
  final val rawAction = ScalaReflection.schemaFor[SingleAction].dataType.asInstanceOf[StructType]
    .add(StructField("fileName", StringType))
    .add(StructField("path", StringType))
    .add(StructField("puid", StringType))
    .add(StructField("commit_version", LongType))
    .add(StructField("updateTs", TimestampType))
    .add(StructField("commitTs", TimestampType))
    .add(StructField("commit_date", DateType))
  final val pathConfig = ScalaReflection.schemaFor[PathConfig].dataType.asInstanceOf[StructType]

}

object ODSSchemas extends ODSchemas
