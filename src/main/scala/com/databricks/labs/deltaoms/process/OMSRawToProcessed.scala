package com.databricks.labs.deltaoms.process

import com.databricks.labs.deltaoms.common.{OMSInitializer, OMSRunner}
import com.databricks.labs.deltaoms.common.OMSUtils._
import com.databricks.labs.deltaoms.model.AddFileInfo
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StructType}

object OMSRawToProcessed extends OMSRunner with OMSInitializer {

  def main(args: Array[String]): Unit = {
    logInfo(s"Starting processing the OMS Raw Data : $omsConfig")
    val rawActionStream = spark.readStream.format("delta").load(rawActionsTablePath)

    val addRemoveStream = rawActionStream
      .where(col("add").isNotNull || col("remove").isNotNull)
      .select(col(PUID), col(COMMIT_VERSION),
        when(col("add").isNotNull,struct(col("add.path"),
          coalesce(col("add.size"), lit(0L)).as("size"),
          coalesce(get_json_object(col("add.stats"),"$.numRecords")
            .cast(LongType),lit(0L)).as("numRecords")))
          .otherwise(lit(null: StructType)).as("add_file"),
        when(col("remove").isNotNull,
          struct(col("remove.path"))).otherwise(lit(null: StructType))
          .as("remove_file")
      )

    val commitInfo = rawActionStream.where("commitInfo is NOT NULL")
      .select(COMMIT_VERSION,COMMIT_TS,"commitInfo.*").drop("version")
    //val metaData =
  }
}
