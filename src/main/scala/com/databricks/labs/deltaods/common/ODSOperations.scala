package com.databricks.labs.deltaods.common

import java.time.{Instant, LocalDateTime, ZoneOffset}

import com.databricks.labs.deltaods.model.{DeltaTableHistory, DeltaTableIdentifierWithLatestVersion, ODSCommitInfo, PathConfig, TablePathWithVersion}
import com.databricks.labs.deltaods.utils.DataFrameOperations._
import com.databricks.labs.deltaods.utils.ODSUtils._
import com.databricks.labs.deltaods.utils.UtilityOperations._
import io.delta.tables._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._

import scala.util.{Failure, Success, Try}

trait ODSOperations extends Serializable with SparkSettings with Logging {
  val implicits = spark.implicits
  import implicits._

  def fetchLastUpdatedVersions(dTables: Seq[DeltaTableIdentifierWithLatestVersion]): Seq[DeltaTableIdentifierWithLatestVersion] = {

    val dTablesPath = dTables.map(_.getTablePathWithVersion(spark))
    val dTablesPathDF = dTablesPath.toDF()
    val odsTableLastVersions = Try {
      spark.table(s"${odsConfig.dbName}.${odsConfig.latestVersionTable}")
    }
    odsTableLastVersions match {
      case Success(i) => {
        val odsLastVersionDF = i
        if (odsLastVersionDF.isEmpty) {
          dTablesPathDF.writeDataToDeltaTable(path=lastVersionTablePath)
          dTables
        } else {
          val tpvs = dTablesPathDF.join(odsLastVersionDF, Seq("path","qualifiedName"),"left_outer")
            .select(dTablesPathDF("path"),
              dTablesPathDF("qualifiedName"),
              coalesce(odsLastVersionDF("version"),dTablesPathDF("version")).as("version"),
              coalesce(dTablesPathDF("refreshTimestamp"),
                odsLastVersionDF("refreshTimestamp")).as("refreshTimestamp"))
            .as[TablePathWithVersion].collect.toSeq

          val updatedDTables: Seq[DeltaTableIdentifierWithLatestVersion] = dTables.map{ dtv =>
            val ftpv = tpvs.find(tpv => (tpv.path == dtv.table.getPath(spark).toString) &&
              (tpv.qualifiedName == dtv.table.unquotedString))
            if(ftpv.isDefined){
              dtv.copy(version=ftpv.get.version)
            } else {
              dtv
            }
          }
          updatedDTables
        }
      }
      case Failure(s) => {
        logInfo(s"Reading ODS last version table failed. Reason: $s")
        throw new RuntimeException(s)
      }
    }
  }

  def updateLastVersions(lastUpdatedDF: Dataset[TablePathWithVersion]) = {
    val lastVersionDeltaTable = Try {
      DeltaTable.forName(s"${odsConfig.dbName}.${odsConfig.latestVersionTable}")
    }
    lastVersionDeltaTable match {
      case Success(luv) => {
        luv.as("luv")
          .merge(lastUpdatedDF.toDF().as("recent_luv"),
          """luv.qualifiedName = recent_luv.qualifiedName and
              |luv.path = recent_luv.path
              |""".stripMargin)
          .whenMatched.updateAll()
          .whenNotMatched.insertAll().execute()
      }
      case Failure(ex) => throw new RuntimeException(s"Unable to update the last version table $ex ")
    }
  }

  def updateTableCommitHistoryToODS(rths: Seq[DeltaTableHistory]): Unit = {

    val recentTableCommitInfoDS: Dataset[ODSCommitInfo] = rths.map(rth => ODSCommitInfo(rth.table.getPathString(spark),
      rth.table.getQualifiedName,
      LocalDateTime.ofInstant(rth.table.refreshTimestamp, ZoneOffset.UTC).toLocalDate,
      rth.history)).toDS()

    val recentTableHistoriesDS = recentTableCommitInfoDS
      .withColumn("commitInfoExploded",explode($"commitInfo"))
      .select($"path",$"qualifiedName",$"commitDate",$"commitInfoExploded.*")

    val rawCommitODSDeltaTable = Try {
      DeltaTable.forName(s"${odsConfig.dbName}.${odsConfig.rawCommitTable}")
    }
    rawCommitODSDeltaTable match {
      case Success(rct) => {
        rct.as("raw_commit")
          .merge(recentTableHistoriesDS.toDF().as("recent_history_updates"),
            """raw_commit.qualifiedName = recent_history_updates.qualifiedName and
                    raw_commit.commitDate = recent_history_updates.commitDate and
                    raw_commit.version = recent_history_updates.version and
                    raw_commit.path = recent_history_updates.path""")
          .whenMatched.updateAll()
          .whenNotMatched.insertAll().execute()

        val latestVersionsDF = recentTableHistoriesDS
          .select("qualifiedName","path","version")
          .groupBy("qualifiedName","path")
          .agg(max("version").as("version"))
          .withColumn("refreshTimestamp", lit(Instant.now())).as[TablePathWithVersion]
        updateLastVersions(latestVersionsDF)
      }
      case Failure(ex) => throw new RuntimeException(s"Unable to update the raw commit table. $ex")
    }
  }

  def updateODSPathConfigFromMetaStore() = {
    val metaStoreDeltaTables = fetchMetaStoreDeltaTables(odsConfig.srcDatabases, odsConfig.tablePattern)
    val tablePaths = metaStoreDeltaTables.map(_.table.getPath(spark).toString).toDS()
    val regex_str = "^(.*)\\/(.*)\\/(.*)\\/(.*)$"
    val wildCardPathsDF = tablePaths
      .withColumn("wildPath",
        regexp_replace($"value",regex_str,"$1/$2/*/*/_delta_log/*.json"))
      .select("wildPath").distinct

    val pathConfigDF = wildCardPathsDF
      .withColumn("uid",substring(sha1($"wildPath"),0,7))
      .withColumnRenamed("wildPath","path")
      .withColumn("automated", lit(true))
      .withColumn("skipProcessing", lit(false))
      .withColumn("updateTs", lit(Instant.now())).as[PathConfig]

    updatePathConfigToODS(pathConfigDF)

  }

  def updatePathConfigToODS(pathConfigs: Dataset[PathConfig]) = {
    val pathConfigODSDeltaTable = Try {
      DeltaTable.forName(s"${odsConfig.dbName}.${odsConfig.pathConfigTable}")
    }
    pathConfigODSDeltaTable match {
      case Success(pct) => {
        pct.as("pathconfig")
          .merge(pathConfigs.toDF().as("pathconfig_updates"),
            """pathconfig.uid = pathconfig_updates.uid""")
          .whenMatched.updateExpr(Map("updateTs" -> "pathconfig_updates.updateTs"))
          .whenNotMatched.insertAll().execute()
      }
      case Failure(ex) => throw new RuntimeException(s"Unable to update the Path Config table. $ex")
    }
  }

  def updateRawCommitHistoryToODS() = {
    val metastoreDeltaTables = fetchMetaStoreDeltaTables(odsConfig.srcDatabases,odsConfig.tablePattern)
    val odsLastVersionForTables = fetchLastUpdatedVersions(metastoreDeltaTables)

    val recentTableHistories = odsLastVersionForTables
      .map(lvt => getHistoryFromTableVersion(lvt,odsConfig.versionFetchSize))

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
}
