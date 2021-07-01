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

package com.databricks.labs.deltaoms.process

import com.databricks.labs.deltaoms.common.BatchOMSRunner
import com.databricks.labs.deltaoms.common.OMSUtils._

object OMSProcessRawActions extends BatchOMSRunner {

  def main(args: Array[String]): Unit = {
    val consolidatedOMSConfig = consolidateAndValidateOMSConfig(args, omsConfig)
    logInfo(s"Starting processing the OMS Raw Data : $consolidatedOMSConfig")
    // Get the current version for Raw Actions
    val currentRawActionsVersion =
      getCurrentRawActionsVersion(getRawActionsTablePath(consolidatedOMSConfig))
    // Get Last OMS Raw Actions Commit Version that was processed
    val lastProcessedRawActionsVersion =
      getLastProcessedRawActionsVersion(getProcessedHistoryTablePath(consolidatedOMSConfig),
        consolidatedOMSConfig.rawActionTable)
    if (currentRawActionsVersion == 0 ||
      lastProcessedRawActionsVersion < currentRawActionsVersion) {
      // Read the changed data since that version
      val newRawActions = getUpdatedRawActions(lastProcessedRawActionsVersion,
        getRawActionsTablePath(consolidatedOMSConfig))
      // Extract and Persist Commit Info from the new Raw Actions
      processCommitInfoFromRawActions(newRawActions,
        getCommitSnapshotTablePath(consolidatedOMSConfig),
        getCommitSnapshotTableName(consolidatedOMSConfig))
      // Extract, Compute Version Snapshots and Persist Action Info from the new Raw Actions
      processActionSnapshotsFromRawActions(newRawActions,
        getActionSnapshotTablePath(consolidatedOMSConfig),
        getActionSnapshotTableName(consolidatedOMSConfig))
      // Find the latest version from the newly processed raw actions
      val latestRawActionVersion = getLatestRawActionsVersion(newRawActions)
      // Update history with the version
      updateLastProcessedRawActions(latestRawActionVersion,
        consolidatedOMSConfig.rawActionTable,
        getProcessedHistoryTablePath(consolidatedOMSConfig))
    }
  }
}
