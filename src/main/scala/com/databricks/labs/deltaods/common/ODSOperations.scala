package com.databricks.labs.deltaods.common

import java.time.{Instant, LocalDateTime, ZoneOffset}

import com.databricks.labs.deltaods.configuration.SparkSettings
import com.databricks.labs.deltaods.model.{DeltaTableHistory, ODSDeltaCommitInfo, PathConfig}
import com.databricks.labs.deltaods.utils.DataFrameOperations._
import com.databricks.labs.deltaods.utils.ODSUtils._
import com.databricks.labs.deltaods.utils.UtilityOperations._
import io.delta.tables._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.delta.actions.SingleAction
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StructType}

import scala.util.{Failure, Success, Try}

trait ODSOperations extends Serializable with SparkSettings with Logging {
  val implicits = spark.implicits
  import implicits._

  def updateLatestVersions(lastUpdatedDF: DataFrame) = {
    val pathConfigDeltaTableOption = Try {
      DeltaTable.forPath(pathConfigTablePath)
    }
    pathConfigDeltaTableOption match {
      case Success(pct) => {
        pct.as("pct")
          .merge(lastUpdatedDF.as("recent_pct"),
          """pct.puid = recent_pct.puid """.stripMargin)
          .whenMatched.updateExpr(Map(
          "updateTs" -> "recent_pct.updateTs",
          "version" -> "recent_pct.version"))
          .execute()
      }
      case Failure(ex) => throw new RuntimeException(s"Unable to update the last version table $ex ")
    }
  }

  def updateTableCommitHistoryToODS(rths: Seq[DeltaTableHistory]): Unit = {

    val recentTableCommitInfoDS: Dataset[ODSDeltaCommitInfo] = rths.map(rth =>
      ODSDeltaCommitInfo(rth.tableConfig.puid, rth.tableConfig.path,
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

    val rawCommitODSDeltaTable = Try {
      DeltaTable.forPath(rawCommitTablePath)
    }
    rawCommitODSDeltaTable match {
      case Success(rct) => {
        rct.as("raw_commit")
          .merge(recentTableHistoriesDS.as("recent_history_updates"),
            """raw_commit.puid = recent_history_updates.puid and
                    raw_commit.commitDate = recent_history_updates.commitDate and
                    raw_commit.version = recent_history_updates.version""")
          .whenMatched.updateAll()
          .whenNotMatched.insertAll().execute()

        val latestVersionsDF = recentTableHistoriesDS
          .select("puid","version")
          .groupBy("puid")
          .agg((max("version")+1).as("version"))
          .withColumn("updateTs", lit(Instant.now()))
        updateLatestVersions(latestVersionsDF)
      }
      case Failure(ex) => throw new RuntimeException(s"Unable to update the raw commit table. $ex")
    }
  }

  def updateODSPathConfigFromMetaStore(truncate: Boolean = false) = {
    val metaStoreDeltaTables = fetchMetaStoreDeltaTables(odsConfig.srcDatabases, odsConfig.tablePattern)
    val deltaWildCardPath  = getDeltaWildCardPathUDF()
    val tablePaths = metaStoreDeltaTables.map(mdt => (mdt.unquotedString, mdt.getPath(spark).toString))
      .toDF("qualifiedName","path")
    val pathConfigDF = tablePaths
      .withColumn("puid",substring(sha1($"path"),0,7))
      .withColumn("wildCardPath",deltaWildCardPath($"path"))
      .withColumn("wuid",substring(sha1($"wildCardPath"),0,7))
      .withColumn("automated", lit(true))
      .withColumn("version",lit(0L))
      .withColumn("skipProcessing", lit(false))
      .withColumn("updateTs", lit(Instant.now())).as[PathConfig]

    updatePathConfigToODS(pathConfigDF, truncate)

  }

  def updatePathConfigToODS(pathConfigs: Dataset[PathConfig], truncate: Boolean = false) = {
    val pathConfigODSDeltaTable = Try {
      DeltaTable.forName(s"${odsConfig.dbName}.${odsConfig.pathConfigTable}")
    }
    pathConfigODSDeltaTable match {
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

  def updateRawCommitHistoryToODS() = {
    val deltaTables = fetchPathConfigForProcessing().collect()
    val recentTableHistories = deltaTables
      .flatMap(lvt => getHistoryFromTableVersion(lvt,odsConfig.versionFetchSize))
    updateTableCommitHistoryToODS(recentTableHistories)

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

  def streamingUpdateRawDeltaActionsToODS() = {
    val uniquePaths = fetchPathForStreamProcessing()
    val combinedFrame = uniquePaths.flatMap(p => fetchStreamingDeltaLogForPath(p))
      .reduce(_ unionByName _)
    val checkpointBaseDir = odsConfig.odsCheckpointBase.getOrElse("dbfs:/ods")
    val checkpointPath = checkpointBaseDir + "/_ods_checkpoints/rawactions"
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
          .withColumn("fileName", input_file_name())
          .withColumn("path",regexp_extract($"fileName",regex_str,1))
          .withColumn("puid",substring(sha1($"path"),0,7))
          .withColumn("commit_version",regexp_extract($"fileName",regex_str,2).cast(LongType))
          .withColumn("updateTs", lit(Instant.now()))
          .withColumn("modTs",file_modification_time($"fileName"))
          .withColumn("commitTs",to_timestamp($"modTs"))
          .withColumn("commit_date",to_date($"commitTs"))
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
