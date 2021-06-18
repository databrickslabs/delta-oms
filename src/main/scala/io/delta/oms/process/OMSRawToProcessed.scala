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

import io.delta.oms.common.{OMSInitializer, OMSRunner}
import io.delta.oms.common.OMSUtils._

import org.apache.spark.sql.functions._

object OMSRawToProcessed extends OMSRunner with OMSInitializer {

  def main(args: Array[String]): Unit = {
    logInfo(s"Starting processing the OMS Raw Data : $omsConfig")
    import sparkSession.implicits._
    val lastProcessedRawActionsVersion = getLastProcessedRawActionsVersion()

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

    processCommitInfoFromRawActions(newRawActions)
    processActionSnapshotsFromRawActions(newRawActions)
    updateLastProcessedRawActions(currentRawActionsVersion)
  }
}
