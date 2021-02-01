package com.databricks.labs.deltaods.common

import com.databricks.labs.deltaods.model.TableIdentifierWithLatestVersion
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types._

trait ODSchemas {

  final val rawCommit = StructType(Seq(
    StructField("version", LongType, nullable = true),
    StructField("timestamp", TimestampType, true),
    StructField("userId", StringType, true),
    StructField("userName", StringType, true),
    StructField("operation", StringType, true),
    StructField("operationParameters", MapType(StringType, StringType, true), true),
    StructField("job",
      StructType(Seq(
        StructField("jobId", StringType, true),
        StructField("jobName", StringType, true),
        StructField("runId", StringType, true),
        StructField("jobOwnerId", StringType, true),
        StructField("triggerType", StringType, true)
      )), true),
    StructField("notebook", StructType(Seq(StructField("notebookId", StringType, true))), true),
    StructField("clusterId", StringType, true),
    StructField("readVersion", LongType, true),
    StructField("isolationLevel", StringType, true),
    StructField("isBlindAppend", BooleanType, true),
    StructField("operationMetrics", MapType(StringType, StringType, true), true),
    StructField("userMetadata", StringType, true),
    StructField("tableName", StringType, true),
    StructField("databaseName", StringType, true),
    StructField("commitDate", DateType, true))
  )

  final val lastVersion = ScalaReflection.schemaFor[TableIdentifierWithLatestVersion].dataType.asInstanceOf[StructType]
  //Encoders.product[TableIdentifierWithLatestVersion].schema
  /*StructType(Seq(
    StructField("databaseName",StringType,true),
    StructField("tableName",StringType,true),
    StructField("lastVersion",LongType,true),
    StructField("refreshTimestamp",TimestampType,true)
    )
  )*/
}

object ODSSchemas extends ODSchemas
