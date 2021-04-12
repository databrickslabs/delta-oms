package com.databricks.labs.deltaoms.common

import java.time.{Instant, LocalDateTime, ZoneOffset}

import com.databricks.labs.deltaoms.configuration.SparkSettings
import com.databricks.labs.deltaoms.model.{PathConfig, TableConfig}
import com.databricks.labs.deltaoms.utils.UtilityOperations._
import com.databricks.labs.deltaoms.common.OMSUtils._
import io.delta.tables._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.delta.actions.SingleAction
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StructType}
import org.apache.spark.sql.streaming.Trigger

import scala.util.{Failure, Random, Success, Try}

trait OMSOperations extends Serializable with SparkSettings with Logging with OMSchemas {
  val implicits = spark.implicits
  import implicits._

  def updatePathConfigWithLatestVersions(lastUpdatedDF: DataFrame) = {
    val pathConfigDeltaTableOption = Try {
      DeltaTable.forPath(pathConfigTablePath)
    }
    pathConfigDeltaTableOption match {
      case Success(pct) => {
        pct.as("pct")
          .merge(lastUpdatedDF.as("recent_pct"),
          s"pct.$PUID = recent_pct.$PUID ")
          .whenMatched.updateExpr(Map(
          s"$UPDATE_TS" -> s"recent_pct.$UPDATE_TS",
          s"$COMMIT_VERSION" -> s"recent_pct.$COMMIT_VERSION"))
          .execute()
      }
      case Failure(ex) => throw new RuntimeException(s"Unable to update the last version table $ex ")
    }
  }

  def fetchTableConfigForProcessing() = {
    val spark = SparkSession.active
    spark.read.format("delta").load(tableConfigPath)
         .where("skipProcessing <> true").select(PATH)
         .as[String]
         .collect()
  }

  def updateOMSPathConfigFromTableConfig() = {
    // Fetch the latest tables configured
    val configuredTables = fetchTableConfigForProcessing()
    //Update the OMS Path Config
    updateOMSPathConfigFromList(configuredTables.toSeq, omsConfig.truncatePathConfig)
  }

  def tablePathToPathConfig(tablePaths: DataFrame) = {
    val deltaWildCardPath  = getDeltaWildCardPathUDF()
    tablePaths
      .withColumn(PUID,substring(sha1($"path"),0,7))
      .withColumn("wildCardPath",deltaWildCardPath($"path"))
      .withColumn(WUID,substring(sha1($"wildCardPath"),0,7))
      .withColumn("automated", lit(false))
      .withColumn(COMMIT_VERSION,lit(0L))
      .withColumn("skipProcessing", lit(false))
      .withColumn(UPDATE_TS, lit(Instant.now())).as[PathConfig]
  }

  def updateOMSPathConfigFromList(locations: Seq[String], truncate: Boolean = false) = {
    val tablePaths = locations.flatMap(validateDeltaLocation).toDF(QUALIFIED_NAME, PATH)
    updatePathConfigToOMS(tablePathToPathConfig(tablePaths), truncate)
  }

  def updateOMSPathConfigFromMetaStore(truncate: Boolean = false) = {
    val metaStoreDeltaTables = fetchMetaStoreDeltaTables(omsConfig.srcDatabases, omsConfig.tablePattern)
    val tablePaths = metaStoreDeltaTables.map(mdt => (mdt.unquotedString, mdt.getPath(spark).toString))
      .toDF(QUALIFIED_NAME,PATH)
    updatePathConfigToOMS(tablePathToPathConfig(tablePaths), truncate)
  }

  def updatePathConfigToOMS(pathConfigs: Dataset[PathConfig], truncate: Boolean = false) = {
    val pathConfigOMSDeltaTable = Try {
      DeltaTable.forName(s"${omsConfig.dbName}.${omsConfig.pathConfigTable}")
    }
    pathConfigOMSDeltaTable match {
      case Success(pct) => {
        if(truncate) pct.delete()
        pct.as("pathconfig")
          .merge(pathConfigs.toDF().as("pathconfig_updates"),
            s"""pathconfig.$PUID = pathconfig_updates.$PUID and
              |pathconfig.$WUID = pathconfig_updates.$WUID
              |""".stripMargin)
          .whenMatched.updateExpr(Map(s"$UPDATE_TS" -> s"pathconfig_updates.$UPDATE_TS"))
          .whenNotMatched.insertAll().execute()
      }
      case Failure(ex) => throw new RuntimeException(s"Unable to update the Path Config table. $ex")
    }
  }

  def fetchPathConfigForProcessing() = {
    val spark = SparkSession.active
    spark.read.format("delta").load(pathConfigTablePath).as[PathConfig]
  }

  def fetchPathForStreamProcessing(useWildCardPath: Boolean = true) ={
    if(useWildCardPath)
      fetchPathConfigForProcessing()
        .select("wildCardPath")
        .distinct().as[String].collect()
    else
      fetchPathConfigForProcessing()
        .select(concat(col("path"),lit("/_delta_log/*.json")).as("path"))
        .distinct().as[String].collect()
  }

  def streamingUpdateRawDeltaActionsToOMS() = {
    val uniquePaths = fetchPathForStreamProcessing(omsConfig.useWildcardPath)
    val combinedFrame = uniquePaths.flatMap(p => fetchStreamingDeltaLogForPath(p))
      .reduce(_ unionByName _)
    val checkpointBaseDir = omsConfig.checkpointBase.getOrElse("dbfs:/tmp/oms")
    val checkpointSuffix = omsConfig.checkpointSuffix.getOrElse(Random.alphanumeric.take(5).mkString)
    val checkpointPath = checkpointBaseDir + "/_oms_checkpoints/raw_actions" + checkpointSuffix
    val triggerInterval = omsConfig.triggerInterval.getOrElse("once")
    val trigger = if(triggerInterval.equalsIgnoreCase("once"))
      Trigger.Once()
    else
      Trigger.ProcessingTime(triggerInterval)

    combinedFrame
      .writeStream
      .partitionBy(puidCommitDatePartitions: _*)
      .outputMode("append")
      .format("delta")
      .option("checkpointLocation", checkpointPath)
      .trigger(trigger)
      .start(rawActionsTablePath)
  }

  def fetchStreamingDeltaLogForPath(path: String, useAutoloader: Boolean = false) = {
    val actionSchema: StructType = ScalaReflection.schemaFor[SingleAction].dataType.asInstanceOf[StructType]
    val regex_str = "^(.*)\\/_delta_log\\/(.*)\\.json$"
    val file_modification_time = getFileModificationTimeUDF()
    if(useAutoloader){
      spark.conf.set("spark.databricks.cloudFiles.schemaInference.enabled", "true")
      Some(spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("failOnUnknownFields", "true")
        .option("unparsedDataColumn", "_unparsed_data")
        //.option("cloudFiles.maxFilesPerTrigger", 1)
        .schema(actionSchema)
        .load(path))
    } else {
      val deltaLogDFOpt = getDeltaLogs(actionSchema, path)
      if(deltaLogDFOpt.nonEmpty){
        val deltaLogDF = deltaLogDFOpt.get
        Some(deltaLogDF
          .withColumn(FILE_NAME, input_file_name())
          .withColumn(PATH,regexp_extract(col(s"$FILE_NAME"),regex_str,1))
          .withColumn(PUID,substring(sha1(col(s"$PATH")),0,7))
          .withColumn(COMMIT_VERSION,regexp_extract(col(s"$FILE_NAME"),regex_str,2).cast(LongType))
          .withColumn(UPDATE_TS, lit(Instant.now()))
          .withColumn("modTs",file_modification_time(col(s"$FILE_NAME")))
          .withColumn(COMMIT_TS,to_timestamp($"modTs"))
          .withColumn(COMMIT_DATE,to_date(col(s"$COMMIT_TS")))
          .drop("modTs"))
      } else {
        None
      }
    }
  }

  def getDeltaLogs(schema: StructType, path: String) = {
    val deltaLogTry = Try {
      spark.readStream.schema(schema).json(path)
    }
    deltaLogTry match {
      case Success(value) => Some(value)
      case Failure(exception) => {
        logError(s"Exception while loading Delta log at $path: $exception")
        None
      }
    }
  }
}
