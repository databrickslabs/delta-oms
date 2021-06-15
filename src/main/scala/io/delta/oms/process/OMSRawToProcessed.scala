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

package io.delta.oms.process

import java.time.Instant

import scala.util.{Failure, Success, Try}
import io.delta.oms.common.{OMSInitializer, OMSRunner}
import io.delta.oms.common.OMSUtils._
import io.delta.tables.DeltaTable

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StructType}
import org.apache.spark.sql.DataFrame

object OMSRawToProcessed extends OMSRunner with OMSInitializer {

  def main(args: Array[String]): Unit = {
    logInfo(s"Starting processing the OMS Raw Data : $omsConfig")
    import sparkSession.implicits._
    val lastProcessedRawActionsVersion = Try {
      spark.read.format("delta")
        .load(processedHistoryTablePath)
        .where(s"tableName='${omsConfig.rawActionTable}'")
        .select("lastVersion").as[Long].head()
    }.getOrElse(0L)
    val currentRawActionsVersion = spark.sql(s"describe history delta.`$rawActionsTablePath`")
      .select(max("version").as("max_version")).as[Long].head()

    val currentRawActions = spark.read.format("delta")
      .option("versionAsOf", currentRawActionsVersion)
      .load(rawActionsTablePath)
    val previousRawActions = spark.read.format("delta")
      .option("versionAsOf", lastProcessedRawActionsVersion)
      .load(rawActionsTablePath)

    val newRawActions = currentRawActions.as("cra")
      .join(previousRawActions.as("pra"), Seq("puid", "commit_version"), "leftanti")

    val newCommitInfo = newRawActions.where(col("commitInfo").isNotNull)
      .selectExpr(COMMIT_VERSION, s"current_timestamp() as $UPDATE_TS",
        COMMIT_TS, FILE_NAME, PATH,
        PUID, COMMIT_DATE, "commitInfo.*").drop("version", "timestamp")

    val commitSnapshotExists = DeltaTable.isDeltaTable(commitSnapshotTablePath)
    if (!commitSnapshotExists) {
      newCommitInfo.write
        .mode("overwrite")
        .format("delta")
        .partitionBy(puidCommitDatePartitions: _*)
        .option("path", commitSnapshotTablePath)
        .saveAsTable(commitSnapshotTableName)
    } else {
      val commitInfoSnapshotTable = Try {
        DeltaTable.forPath(commitSnapshotTablePath)
      }
      commitInfoSnapshotTable match {
        case Success(cst) =>
          cst.as("commitinfo_snap")
            .merge(newCommitInfo.as("commitinfo_snap_updates"),
              s"""commitinfo_snap.$PUID = commitinfo_snap_updates.$PUID and
                 |commitinfo_snap.$COMMIT_DATE = commitinfo_snap_updates.$COMMIT_DATE and
                 |commitinfo_snap.$COMMIT_VERSION = commitinfo_snap_updates.$COMMIT_VERSION
                 |""".stripMargin)
            .whenNotMatched.insertAll().execute()
        case Failure(ex) => throw new RuntimeException(s"Unable to update the Commit Info " +
          s"Snapshot table. $ex")
      }
    }
    val actionSnapshotExists = DeltaTable.isDeltaTable(actionSnapshotTablePath)
    val actionSnapshots = computeActionSnapshotFromRawActions(newRawActions, actionSnapshotExists)
    if (!actionSnapshotExists) {
      actionSnapshots.write
        .mode("overwrite")
        .format("delta")
        .partitionBy(puidCommitDatePartitions: _*)
        .option("overwriteSchema", "true")
        .option("path", actionSnapshotTablePath)
        .saveAsTable(actionSnapshotTableName)
    } else {
      val actionSnapshotTable = Try {
        DeltaTable.forPath(actionSnapshotTablePath)
      }
      actionSnapshotTable match {
        case Success(ast) =>
          ast.as("action_snap")
            .merge(actionSnapshots.as("action_snap_updates"),
              s"""action_snap.$PUID = action_snap_updates.$PUID and
                 |action_snap.$COMMIT_DATE = action_snap_updates.$COMMIT_DATE and
                 |action_snap.$COMMIT_VERSION = action_snap_updates.$COMMIT_VERSION
                 |""".stripMargin)
            .whenNotMatched.insertAll().execute()
        case Failure(ex) => throw new RuntimeException(s"Unable to update the " +
          s"Action Snapshot table. $ex")
      }
    }


    val updatedRawActionsLastProcessedVersion =
      Seq((s"${omsConfig.rawActionTable}", currentRawActionsVersion, Instant.now()))
      .toDF("tableName", "lastVersion", "update_ts")

    val processedHistoryTable = Try {
      DeltaTable.forPath(processedHistoryTablePath)
    }
    processedHistoryTable match {
      case Success(pht) =>
        pht.as("processed_history")
          .merge(updatedRawActionsLastProcessedVersion.as("processed_history_updates"),
            """processed_history.tableName = processed_history_updates.tableName""".stripMargin)
          .whenMatched().updateAll()
          .whenNotMatched.insertAll().execute()
      case Failure(ex) => throw new RuntimeException(s"Unable to update the " +
        s"Processed History table. $ex")
    }
  }

  def computeActionSnapshotFromRawActions(rawActions: org.apache.spark.sql.DataFrame,
    snapshotExists: Boolean): DataFrame = {
    val addRemoveFileActions = prepareAddRemoveActionsFromRawActions(rawActions)
    val cumulativeAddRemoveFiles = if (snapshotExists) {
      val previousSnapshot = spark.read.format("delta").load(actionSnapshotTablePath)
      val previousSnapshotMaxCommitVersion = previousSnapshot.groupBy(PUID)
        .agg(max(COMMIT_VERSION).as(COMMIT_VERSION))
      val previousSnapshotMaxAddRemoveFileActions = previousSnapshot
        .join(previousSnapshotMaxCommitVersion, Seq(PUID, COMMIT_VERSION))
        .withColumn("remove_file", lit(null: StructType))
      val cumulativeAddRemoveFileActions =
        computeCumulativeFilesFromAddRemoveActions(
          addRemoveFileActions.unionByName(previousSnapshotMaxAddRemoveFileActions))
      cumulativeAddRemoveFileActions
        .join(previousSnapshotMaxCommitVersion, Seq(PUID, COMMIT_VERSION), "leftanti")
        .select(cumulativeAddRemoveFileActions("*"))
    } else {
      computeCumulativeFilesFromAddRemoveActions(addRemoveFileActions)
    }
    deriveActionSnapshotFromCumulativeActions(cumulativeAddRemoveFiles)
  }

  def prepareAddRemoveActionsFromRawActions(rawActions: org.apache.spark.sql.DataFrame)
  : DataFrame = {
    val addFileActions = rawActions
      .where(col("add.path").isNotNull)
      .selectExpr("add", "remove", PUID, s"$PATH as data_path",
        COMMIT_VERSION, COMMIT_TS, COMMIT_DATE)

    val duplicateAddWindow =
      Window.partitionBy(col(PUID), col("add.path"))
        .orderBy(col(COMMIT_VERSION).desc_nulls_last)
    // Duplicate AddFile actions could be present under rare circumstances
    val rankedAddFileActions = addFileActions
      .withColumn("rank", rank().over(duplicateAddWindow))
    val dedupedAddFileActions = rankedAddFileActions
      .where("rank = 1").drop("rank")

    val removeFileActions = rawActions
      .where(col("remove.path").isNotNull)
      .selectExpr("add", "remove", PUID, s"$PATH as data_path", COMMIT_VERSION,
        COMMIT_TS, COMMIT_DATE)

    val addRemoveFileActions = dedupedAddFileActions.unionByName(removeFileActions)
      .select(col(PUID), col("data_path"), col(COMMIT_VERSION), col(COMMIT_TS),
        col(COMMIT_DATE),
        when(col("add").isNotNull, struct(col("add.path"),
          coalesce(col("add.size"), lit(0L)).as("size"),
          coalesce(get_json_object(col("add.stats"), "$.numRecords")
            .cast(LongType), lit(0L)).as("numRecords")))
          .otherwise(lit(null: StructType)).as("add_file"),
        when(col("remove.path").isNotNull,
          struct(col("remove.path"))).otherwise(lit(null: StructType))
          .as("remove_file"))
    addRemoveFileActions
  }

  def computeCumulativeFilesFromAddRemoveActions(addRemoveActions: org.apache.spark.sql.DataFrame)
  : DataFrame = {
    val commitVersions = addRemoveActions.select(PUID, COMMIT_VERSION, COMMIT_TS, COMMIT_DATE)
      .distinct()
    val cumulativeAddRemoveFiles = addRemoveActions.as("arf")
      .join(commitVersions.as("cv"), col("arf.puid") === col("cv.puid")
      && col(s"arf.$COMMIT_VERSION") <= col(s"cv.$COMMIT_VERSION"))
      .select(col(s"cv.$COMMIT_VERSION"), col(s"cv.$COMMIT_TS"),
        col(s"cv.$COMMIT_DATE"),
        col(s"arf.$PUID"), col("arf.data_path"),
        col("arf.add_file"), col("arf.remove_file"))
    cumulativeAddRemoveFiles
  }

  def deriveActionSnapshotFromCumulativeActions(
    cumulativeAddRemoveFiles: org.apache.spark.sql.DataFrame): DataFrame = {
    val cumulativeAddFiles = cumulativeAddRemoveFiles
      .where(col("add_file.path").isNotNull)
      .drop("remove_file")
    val cumulativeRemoveFiles = cumulativeAddRemoveFiles
      .where(col("remove_file.path").isNotNull)
      .drop("add_file")
    val snapshotInputFiles = cumulativeAddFiles.as("ca")
      .join(cumulativeRemoveFiles.as("cr"),
      col(s"ca.$PUID") === col(s"cr.$PUID")
        && col(s"ca.$COMMIT_VERSION") === col(s"cr.$COMMIT_VERSION")
        && col("ca.add_file.path") ===
        col("cr.remove_file.path"), "leftanti").selectExpr("ca.*")
    snapshotInputFiles
  }
}
