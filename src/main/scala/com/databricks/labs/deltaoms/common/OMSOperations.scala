package com.databricks.labs.deltaoms.common

import java.time.{Instant, LocalDateTime, ZoneOffset}

import com.databricks.labs.deltaoms.configuration.SparkSettings
import com.databricks.labs.deltaoms.model.{DeltaTableHistory, OMSDeltaCommitInfo, PathConfig}
import com.databricks.labs.deltaoms.utils.UtilityOperations._
import com.databricks.labs.deltaoms.common.OMSUtils._
import io.delta.tables._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.delta.actions.SingleAction
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StructType}

import scala.util.{Failure, Success, Try}

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

  def updateTableCommitHistoryToOMS(rths: Seq[DeltaTableHistory]): Unit = {

    val recentTableCommitInfoDS: Dataset[OMSDeltaCommitInfo] = rths.map(rth =>
      OMSDeltaCommitInfo(rth.tableConfig.puid, rth.tableConfig.path,
      rth.tableConfig.qualifiedName,
        Instant.now(),
      rth.history)).toDS()

    val recentTableHistoriesDS = recentTableCommitInfoDS
      .withColumn("commitInfoExploded",explode($"commitInfo"))
      .select($"puid",$"path",$"qualifiedName",$"updateTs",$"commitInfoExploded.*")
      .withColumn("commitDate", to_date($"timestamp"))

    /*recentTableHistoriesDS
      .write
      .partitionBy(rawCommitPartitions: _*)
      .mode("append")
      .format("delta")
      .save(rawCommitTablePath)

    val latestVersionsDF = recentTableHistoriesDS
      .select("puid","version")
      .groupBy("puid")
      .agg(max("version").as("version"))
      .withColumn("updateTs", lit(Instant.now()))
    updateLatestVersions(latestVersionsDF)*/

    val rawCommitOMSDeltaTable = Try {
      DeltaTable.forPath(rawCommitTablePath)
    }
    rawCommitOMSDeltaTable match {
      case Success(rct) => {
        rct.as("raw_commit")
          .merge(recentTableHistoriesDS.as("recent_history_updates"),
            """raw_commit.puid = recent_history_updates.puid and
                    raw_commit.commitDate = recent_history_updates.commitDate and
                    raw_commit.version = recent_history_updates.version""")
          .whenMatched.updateAll()
          .whenNotMatched.insertAll().execute()

        val latestVersionsDF = recentTableHistoriesDS
          .select(PUID,COMMIT_VERSION)
          .groupBy(PUID)
          .agg((max(COMMIT_VERSION)+1).as(COMMIT_VERSION))
          .withColumn(UPDATE_TS, lit(Instant.now()))
        updatePathConfigWithLatestVersions(latestVersionsDF)
      }
      case Failure(ex) => throw new RuntimeException(s"Unable to update the raw commit table. $ex")
    }
  }

  def updateOMSPathConfigFromMetaStore(truncate: Boolean = false) = {
    val metaStoreDeltaTables = fetchMetaStoreDeltaTables(omsConfig.srcDatabases, omsConfig.tablePattern)
    val deltaWildCardPath  = getDeltaWildCardPathUDF()
    val tablePaths = metaStoreDeltaTables.map(mdt => (mdt.unquotedString, mdt.getPath(spark).toString))
      .toDF(QUALIFIED_NAME,PATH)
    val pathConfigDF = tablePaths
      .withColumn(PUID,substring(sha1($"path"),0,7))
      .withColumn("wildCardPath",deltaWildCardPath($"path"))
      .withColumn("wuid",substring(sha1($"wildCardPath"),0,7))
      .withColumn("automated", lit(true))
      .withColumn(COMMIT_VERSION,lit(0L))
      .withColumn("skipProcessing", lit(false))
      .withColumn(UPDATE_TS, lit(Instant.now())).as[PathConfig]

    updatePathConfigToOMS(pathConfigDF, truncate)

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
            """pathconfig.puid = pathconfig_updates.puid and
              |pathconfig.wuid = pathconfig_updates.wuid
              |""".stripMargin)
          .whenMatched.updateExpr(Map("updateTs" -> "pathconfig_updates.updateTs"))
          .whenNotMatched.insertAll().execute()
      }
      case Failure(ex) => throw new RuntimeException(s"Unable to update the Path Config table. $ex")
    }
  }

  def fetchPathConfigForProcessing() = {
    val spark = SparkSession.active
    spark.read.format("delta").load(pathConfigTablePath).as[PathConfig]
  }

  def fetchPathForStreamProcessing() ={
    fetchPathConfigForProcessing().select("wildCardPath").distinct().as[String].collect()
  }

  def updateRawCommitHistoryToOMS() = {
    val deltaTables = fetchPathConfigForProcessing().collect()
    val recentTableHistories = deltaTables
      .flatMap(lvt => getHistoryFromTableVersion(lvt,omsConfig.versionFetchSize))
    updateTableCommitHistoryToOMS(recentTableHistories)

    /*val recentTableHistories = odsLastVersionForTables
      .map(dt => ParallelAsyncExecutor.executeAsync(
        getHistoryFromTableVersion(TableDefinition(dt.tableName,
          dt.databaseName,
          None,
          dt.,
          Some("ODS Last Table Version"),
          odsProperties
        ))))
      .grouped(2)
      .map(it => ParallelAsyncExecutor.awaitSliding(it.iterator))
      .flatten*/
  }

  def streamingUpdateRawDeltaActionsToOMS() = {
    val uniquePaths = fetchPathForStreamProcessing()
    val combinedFrame = uniquePaths.flatMap(p => fetchStreamingDeltaLogForPath(p))
      .reduce(_ unionByName _)
    val checkpointBaseDir = omsConfig.omsCheckpointBase.getOrElse("dbfs:/oms")
    val checkpointPath = checkpointBaseDir + "/_oms_checkpoints/rawactions"
    combinedFrame
      .writeStream
      .partitionBy(rawActionsPartitions: _*)
      .outputMode("append")
      .format("delta")
      .option("checkpointLocation", checkpointPath)
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
