/*
 * Copyright (2021) Databricks, Inc.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE
 * AND NONINFRINGEMENT.
 *
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR
 * THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 * See the Full License for the specific language governing permissions and
 * limitations under the License.
 */

package com.databricks.labs.deltaoms.common

import java.time.Instant

import scala.util.{Failure, Success, Try}
import com.databricks.labs.deltaoms.configuration.{OMSConfig, SparkSettings}
import com.databricks.labs.deltaoms.model.{PathConfig, SourceConfig, StreamTargetInfo}
import Utils._
import com.databricks.labs.deltaoms.utils.UtilityOperations._
import io.delta.tables._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.delta.actions.SingleAction
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.types.{LongType, StructType}
import org.apache.spark.util.SerializableConfiguration

trait OMSOperations extends Serializable with SparkSettings with Logging with Schemas {
  val implicits = spark.implicits

  import implicits._

  def updateOMSPathConfigFromSourceConfig(config: OMSConfig): Unit = {
    // Fetch the latest tables configured
    val configuredSources: Array[SourceConfig] = fetchSourceConfigForProcessing(config)
    // Update the OMS Path Config
    updateOMSPathConfigFromList(configuredSources.toSeq,
      getPathConfigTableName(config),
      config.truncatePathConfig)
  }

  def processWildcardDirectories(sourceConfigs: DataFrame): Dataset[SourceConfig] = {
    val spark = SparkSession.active
    val hadoopConf = new SerializableConfiguration(spark.sessionState.newHadoopConf())

    val nonWildCardSourcePaths = sourceConfigs
      .filter(substring(col(PATH), -2, 2) =!= "**").as[SourceConfig]
    val wildCardSourcePaths = sourceConfigs
      .filter(substring(col(PATH), -2, 2) === "**")
      .selectExpr(s"substring($PATH,1,length($PATH)-2) as $PATH",
        s"$SKIP_PROCESSING").as[SourceConfig]
    val wildCardSubDirectories = wildCardSourcePaths.flatMap(listSubDirectories(_, hadoopConf))
    val wildCardTablePaths = wildCardSubDirectories.repartition(32)
      .flatMap(recursiveListDeltaTablePaths(_, hadoopConf))
    wildCardTablePaths.unionByName(nonWildCardSourcePaths)
  }

  def fetchSourceConfigForProcessing(config: OMSConfig): Array[SourceConfig] = {
    val spark = SparkSession.active
    val sourceConfigs = spark.read.table(getSourceConfigTableName(config))
      .where(s"$SKIP_PROCESSING <> true").select(PATH, SKIP_PROCESSING)
    processWildcardDirectories(sourceConfigs).collect()
  }

  def updateOMSPathConfigFromList(sourceConfigs: Seq[SourceConfig],
    pathConfigTableName: String,
    truncate: Boolean = false)
  : Unit = {
    val tablePaths: DataFrame = sourceConfigs.flatMap(resolveDeltaLocation).filter(_._2.isDefined)
      .toDF(QUALIFIED_NAME, PATH, PARAMETERS)
    updatePathConfigToOMS(tablePathToPathConfig(tablePaths),
      pathConfigTableName,
      truncate)
  }

  def tablePathToPathConfig(tablePaths: DataFrame): Dataset[PathConfig] = {
    val deltaWildCardPath = getDeltaWildCardPathUDF()
    tablePaths
      .withColumn(PUID, substring(sha1($"path"), 0, 7))
      .withColumn("wildCardPath",
        deltaWildCardPath(col(s"$PATH"), col(s"$PARAMETERS.$WILDCARD_LEVEL")))
      .withColumn(WUID, substring(sha1($"wildCardPath"), 0, 7))
      .withColumn("skipProcessing", lit(false))
      .withColumn(UPDATE_TS, lit(Instant.now())).as[PathConfig]
  }

  def updatePathConfigToOMS(pathConfigs: Dataset[PathConfig],
    pathConfigTableName: String,
    truncate: Boolean = false): Unit = {
    val pathConfigOMSDeltaTable = Try {
      DeltaTable.forName(pathConfigTableName)
    }
    pathConfigOMSDeltaTable match {
      case Success(pct) =>
        if (truncate) pct.delete()
        pct.as("pathconfig")
          .merge(pathConfigs.toDF().as("pathconfig_updates"),
            s"""pathconfig.$PUID = pathconfig_updates.$PUID and
               |pathconfig.$WUID = pathconfig_updates.$WUID
               |""".stripMargin)
          .whenMatched.updateExpr(Map(s"$UPDATE_TS" -> s"pathconfig_updates.$UPDATE_TS"))
          .whenNotMatched.insertAll().execute()
      case Failure(ex) => throw new RuntimeException(s"Unable to update the Path Config table. $ex")
    }
  }

  def insertRawDeltaLogs(rawActionsTableUrl: String)(newDeltaLogDF: DataFrame, batchId: Long):
  Unit = {
    newDeltaLogDF.cache()

    val puids = newDeltaLogDF.select(PUID).distinct().as[String].collect()
      .mkString("'", "','", "'")
    val commitDates = newDeltaLogDF.select(COMMIT_DATE).distinct().as[String].collect()
      .mkString("'", "','", "'")

    val rawActionsTable = Try {
      DeltaTable.forPath(rawActionsTableUrl)
    }

    rawActionsTable match {
      case Success(rat) =>
        rat.as("raw_actions")
          .merge(newDeltaLogDF.as("raw_actions_updates"),
            s"""raw_actions.$PUID = raw_actions_updates.$PUID and
               |raw_actions.$PUID in ($puids) and
               |raw_actions.$COMMIT_DATE in ($commitDates) and
               |raw_actions.$COMMIT_DATE = raw_actions_updates.$COMMIT_DATE and
               |raw_actions.$COMMIT_VERSION = raw_actions_updates.$COMMIT_VERSION
               |""".stripMargin)
          .whenNotMatched.insertAll().execute()
      case Failure(ex) => throw new RuntimeException(s"Unable to insert new data into " +
        s"Raw Actions table. $ex")
    }
    newDeltaLogDF.unpersist()
  }

  def processDeltaLogStreams(streamTargetAndLog: (DataFrame, StreamTargetInfo),
    triggerIntervalOption: Option[String]): (String, StreamingQuery) = {
    val readStream = streamTargetAndLog._1
    val targetInfo = streamTargetAndLog._2
    assert(targetInfo.wuid.isDefined, "OMS Readstreams should be associated with WildcardPath")
    val triggerInterval = triggerIntervalOption.getOrElse("availableNow")
    val trigger = if (triggerInterval.equalsIgnoreCase("availableNow") || triggerInterval.equalsIgnoreCase("once")) { // scalastyle:ignore
      Trigger.AvailableNow()
    } else {
      Trigger.ProcessingTime(triggerInterval)
    }
    val wuid = targetInfo.wuid.get
    val poolName = "pool_" + wuid
    val queryName = "query_" + wuid
    val targetTableUrl = targetInfo.url

    spark.sparkContext.setLocalProperty("spark.scheduler.pool", poolName)
    (wuid, readStream
      .writeStream
      .format("delta")
      .queryName(queryName)
      .foreachBatch(insertRawDeltaLogs(targetTableUrl) _)
      .outputMode("update")
      .option("checkpointLocation", targetInfo.checkpointPath)
      .trigger(trigger)
      .start())
  }

  def streamingUpdateRawDeltaActionsToOMS(config: OMSConfig): Unit = {
    val uniquePaths: Seq[(String, String)] = if (config.consolidateWildcardPaths) {
      consolidateWildCardPaths(
        fetchPathForStreamProcessing(getPathConfigTableUrl(config),
          startingStream = config.startingStream, endingStream = config.endingStream))
    } else {
      fetchPathForStreamProcessing(getPathConfigTableUrl(config),
        startingStream = config.startingStream, endingStream = config.endingStream)
    }

    val logReadStreams: Seq[(DataFrame, StreamTargetInfo)] = uniquePaths.flatMap(p =>
      fetchStreamTargetAndDeltaLogForPath(p,
        config.checkpointBase.get,
        config.checkpointSuffix.get,
        getRawActionsTableUrl(config), config.useAutoloader, config.maxFilesPerTrigger))

    val logWriteStreamQueries = logReadStreams
      .map(lrs => processDeltaLogStreams(lrs,
        config.triggerInterval))

    spark.streams.addListener(new OMSStreamingQueryListener())
    logWriteStreamQueries.foreach(x => x._2.status.prettyJson)
    spark.streams.awaitAnyTermination()
  }


  def fetchPathForStreamProcessing(pathConfigTableUrl: String,
    useWildCardPath: Boolean = true, startingStream: Int = 1, endingStream: Int = 50):
  Seq[(String, String)] = {
    if (useWildCardPath) {
      val wildcard_window = Window.orderBy(WUID)
      fetchPathConfigForProcessing(pathConfigTableUrl)
        .select(WILDCARD_PATH, WUID)
        .distinct()
        .withColumn("wildcard_row_id", row_number().over(wildcard_window))
        .where($"wildcard_row_id".between(startingStream, endingStream))
        .drop("wildcard_row_id")
        .as[(String, String)].collect()
    } else {
      val path_window = Window.orderBy(PUID)
      fetchPathConfigForProcessing(pathConfigTableUrl)
        .select(concat(col(PATH), lit("/_delta_log/*.json")).as(PATH), col(PUID))
        .distinct()
        .withColumn("path_row_id", row_number().over(path_window))
        .where($"path_row_id".between(startingStream, endingStream))
        .drop("path_row_id")
        .as[(String, String)].collect()
    }
  }

  def fetchPathConfigForProcessing(pathConfigTableUrl: String): Dataset[PathConfig] = {
    val spark = SparkSession.active
    spark.read.format("delta").load(pathConfigTableUrl).as[PathConfig]
  }

  def fetchStreamTargetAndDeltaLogForPath(pathInfo: (String, String),
    checkpointBaseDir: String, checkpointSuffix: String, rawActionsTableUrl: String,
    useAutoLoader: Boolean, maxFilesPerTrigger: String):
  Option[(DataFrame, StreamTargetInfo)] = {
    val wildCardPath = pathInfo._1
    val wuid = pathInfo._2
    val checkpointPath = checkpointBaseDir + "/_oms_checkpoints/raw_actions_" +
      wuid + checkpointSuffix

    val readPathStream = fetchStreamingDeltaLogForPath(wildCardPath, useAutoLoader,
      maxFilesPerTrigger)
    if(readPathStream.isDefined) {
      Some(readPathStream.get,
        StreamTargetInfo(url = rawActionsTableUrl, checkpointPath = checkpointPath,
          wuid = Some(wuid)))
    } else {
      None
    }
  }

  def fetchStreamingDeltaLogForPath(path: String, useAutoloader: Boolean = true,
    maxFilesPerTrigger: String = "1024")
  : Option[DataFrame] = {
    val actionSchema: StructType = ScalaReflection.schemaFor[SingleAction].dataType
      .asInstanceOf[StructType]
    val regex_str = "^(.*)\\/_delta_log\\/(.*)\\.json$"
    val deltaLogDFOpt = if (useAutoloader) {
      Some(spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.maxFilesPerTrigger", maxFilesPerTrigger)
        .option("cloudFiles.useIncrementalListing", "true")
        .schema(actionSchema)
        .load(path).select("*", "_metadata"))
    } else {
      getDeltaLogs(actionSchema, path, maxFilesPerTrigger)
    }
    if (deltaLogDFOpt.nonEmpty) {
        val deltaLogDF = deltaLogDFOpt.get
          .withColumn(FILE_NAME, col("_metadata.file_path"))
          .withColumn(COMMIT_TS, col("_metadata.file_modification_time"))
        Some(deltaLogDF
          .withColumn(PATH, regexp_extract(col(s"$FILE_NAME"), regex_str, 1))
          .withColumn(PUID, substring(sha1(col(s"$PATH")), 0, 7))
          .withColumn(COMMIT_VERSION, regexp_extract(col(s"$FILE_NAME"),
            regex_str, 2).cast(LongType))
          .withColumn(UPDATE_TS, lit(Instant.now()))
          .withColumn(COMMIT_DATE, to_date(col(s"$COMMIT_TS")))
          .drop("_metadata"))
      } else {
        None
      }
  }

  def getCurrentRawActionsVersion(rawActionsTableUrl: String): Long = {
    val rawActionsTableTry = Try {
      DeltaTable.forPath(rawActionsTableUrl)
    }
    rawActionsTableTry match {
      case Success(rat) => rat.history(1)
        .selectExpr("version as max_version").as[Long].head()
      case Failure(ex) => throw new RuntimeException(s"Unable to access the " +
        s"RawActions table for getting current RawActions version. $ex")
    }
  }

  def getLastProcessedRawActionsVersion(processedHistoryTableUrl: String,
    rawActionTable: String): Long = {
    Try {
      spark.read.format("delta").load(processedHistoryTableUrl)
        .where(s"tableName='$rawActionTable'")
        .select("lastVersion").as[Long].head()
    }.getOrElse(0L)
  }

  def getLatestRawActionsVersion(rawActions: DataFrame): Long = {
    Try {
      rawActions.select(max(s"_$COMMIT_VERSION")).as[Long].head()
    }.getOrElse(0L)
  }

  def updateLastProcessedRawActions(latestVersion: Long,
    rawActionTable: String,
    processedHistoryTableUrl: String ): Unit = {
    val updatedRawActionsLastProcessedVersion =
      Seq((rawActionTable, latestVersion, Instant.now()))
        .toDF("tableName", "lastVersion", "update_ts")

    val processedHistoryTable = Try {
      DeltaTable.forPath(processedHistoryTableUrl)
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

  def getUpdatedRawActions(lastProcessedVersion: Long, rawActionsTableUrl: String): DataFrame = {
    spark.read.format("delta")
      .option("readChangeFeed", "true")
      .option("startingVersion", lastProcessedVersion + 1)
      .load(s"$rawActionsTableUrl")
      .filter("""_change_type IN ("insert", "update_postimage")""")
  }

  def processCommitInfoFromRawActions(rawActions: DataFrame,
    commitSnapshotTableUrl: String): Unit = {
    val commitInfo = rawActions.where(col("commitInfo.operation").isNotNull)
      .selectExpr("commitInfo.*",
        FILE_NAME, PATH, COMMIT_VERSION, COMMIT_TS,
        s"current_timestamp() as $UPDATE_TS", PUID, COMMIT_DATE)

    val commitInfoSnapshotTable = Try {
        DeltaTable.forPath(commitSnapshotTableUrl)
    }
    commitInfoSnapshotTable match {
      case Success(cst) =>
        cst.as("commitinfo_snap")
          .merge(commitInfo.as("commitinfo_snap_updates"),
            s"""commitinfo_snap.$PUID = commitinfo_snap_updates.$PUID and
               |commitinfo_snap.$COMMIT_DATE = commitinfo_snap_updates.$COMMIT_DATE and
               |commitinfo_snap.$COMMIT_VERSION = commitinfo_snap_updates.$COMMIT_VERSION
               |""".stripMargin)
          .whenNotMatched.insertAll().execute()
      case Failure(ex) => throw new RuntimeException(s"Unable to update the Commit Info " +
        s"Snapshot table. $ex")
    }
  }

  def processActionSnapshotsFromRawActions(rawActions: DataFrame,
    actionSnapshotTableUrl: String): Unit = {
    val actionSnapshotsTableTry = Try {
      DeltaTable.forPath(actionSnapshotTableUrl)
    }
    val actionSnapshotEmpty = actionSnapshotsTableTry match {
      case Success(cst) => cst.toDF.isEmpty
      case Failure(ex) =>
        throw new RuntimeException(s"Unable to access the ActionSnapshot Table : $ex")
    }
    val actionSnapshots = computeActionSnapshotFromRawActions(rawActions,
      actionSnapshotEmpty,
      actionSnapshotTableUrl)

    actionSnapshotsTableTry match {
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

  def computeActionSnapshotFromRawActions(rawActions: org.apache.spark.sql.DataFrame,
    snapshotEmpty: Boolean, actionSnapshotTableUrl: String): DataFrame = {
    val addRemoveFileActions = prepareAddRemoveActionsFromRawActions(rawActions)
    val cumulativeAddRemoveFiles = if (!snapshotEmpty) {
      val previousSnapshot = spark.read.format("delta")
        .load(actionSnapshotTableUrl).drop(UPDATE_TS)
      val previousSnapshotMaxCommitVersion = previousSnapshot.groupBy(PUID)
        .agg(max(COMMIT_VERSION).as(COMMIT_VERSION))
      val previousSnapshotMaxAddRemoveFileActions = previousSnapshot
        .join(previousSnapshotMaxCommitVersion, Seq(PUID, COMMIT_VERSION))
        .withColumn(REMOVE, lit(null: StructType))
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
      .where(col(s"$ADD.path").isNotNull)
      .selectExpr(ADD, REMOVE, PUID, s"$PATH as $DATA_PATH",
        COMMIT_VERSION, COMMIT_TS, COMMIT_DATE)

    val duplicateAddWindow =
      Window.partitionBy(col(PUID), col(s"$ADD.path"))
        .orderBy(col(COMMIT_VERSION).desc_nulls_last)
    // Duplicate AddFile actions could be present under rare circumstances
    val rankedAddFileActions = addFileActions
      .withColumn("rank", rank().over(duplicateAddWindow))
    val dedupedAddFileActions = rankedAddFileActions
      .where("rank = 1").drop("rank")

    val removeFileActions = rawActions
      .where(col(s"$REMOVE.path").isNotNull)
      .selectExpr(ADD, REMOVE, PUID, s"$PATH as $DATA_PATH", COMMIT_VERSION,
        COMMIT_TS, COMMIT_DATE)

    val addRemoveFileActions = dedupedAddFileActions.unionByName(removeFileActions)
      .select(col(PUID), col(DATA_PATH), col(COMMIT_VERSION), col(COMMIT_TS),
        col(COMMIT_DATE), col(ADD), col(REMOVE))
    addRemoveFileActions
  }

  def computeCumulativeFilesFromAddRemoveActions(addRemoveActions: org.apache.spark.sql.DataFrame)
  : DataFrame = {
    val commitVersions = addRemoveActions.select(PUID, COMMIT_VERSION, COMMIT_TS, COMMIT_DATE)
      .distinct()
    val cumulativeAddRemoveFiles = addRemoveActions.as("arf")
      .join(commitVersions.as("cv"), col(s"arf.$PUID") === col(s"cv.$PUID")
        && col(s"arf.$COMMIT_VERSION") <= col(s"cv.$COMMIT_VERSION"))
      .select(col(s"cv.$COMMIT_VERSION"), col(s"cv.$COMMIT_TS"),
        col(s"cv.$COMMIT_DATE"),
        col(s"arf.$PUID"), col(s"arf.$DATA_PATH"),
        col(s"arf.$ADD"), col(s"arf.$REMOVE"))
    cumulativeAddRemoveFiles
  }

  def deriveActionSnapshotFromCumulativeActions(
    cumulativeAddRemoveFiles: org.apache.spark.sql.DataFrame): DataFrame = {
    val cumulativeAddFiles = cumulativeAddRemoveFiles
      .where(col(s"$ADD.$PATH").isNotNull)
      .drop(REMOVE)
    val cumulativeRemoveFiles = cumulativeAddRemoveFiles
      .where(col(s"$REMOVE.$PATH").isNotNull)
      .drop(ADD)
    val snapshotInputFiles = cumulativeAddFiles.as("ca")
      .join(cumulativeRemoveFiles.as("cr"),
        col(s"ca.$PUID") === col(s"cr.$PUID")
          && col(s"ca.$COMMIT_VERSION") === col(s"cr.$COMMIT_VERSION")
          && col(s"ca.$ADD.$PATH") ===
          col(s"cr.$REMOVE.$PATH"), "leftanti").selectExpr("ca.*")
    snapshotInputFiles.select(ADD, DATA_PATH, COMMIT_VERSION, COMMIT_TS, PUID, COMMIT_DATE)
      .withColumn(UPDATE_TS, current_timestamp())
  }

  def getDeltaLogs(schema: StructType, path: String,
    maxFilesPerTrigger: String = "1024"): Option[DataFrame] = {
    val deltaLogTry = Try {
      spark.readStream.schema(schema)
        .option("maxFilesPerTrigger", maxFilesPerTrigger)
        .json(path).select("*", "_metadata")
    }
    deltaLogTry match {
      case Success(value) => Some(value)
      case Failure(exception) =>
        logError(s"Exception while loading Delta log at $path: $exception")
        None
    }
  }
}

object OMSOperations extends OMSOperations
