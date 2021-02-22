package com.databricks.labs.deltaods.common

import com.databricks.labs.deltaods.model.{PathConfig, TablePathWithVersion}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.delta.actions.CommitInfo
import org.apache.spark.sql.types._

trait ODSchemas {
  final val rawCommit = ScalaReflection.schemaFor[CommitInfo].dataType.asInstanceOf[StructType]
    .add(StructField("path", StringType))
    .add(StructField("qualifiedName", StringType))
    .add(StructField("commitDate", DateType))
  final val lastVersion = ScalaReflection.schemaFor[TablePathWithVersion].dataType.asInstanceOf[StructType]
  final val pathConfig = ScalaReflection.schemaFor[PathConfig].dataType.asInstanceOf[StructType]
}

object ODSSchemas extends ODSchemas
