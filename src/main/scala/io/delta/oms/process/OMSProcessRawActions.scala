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

object OMSProcessRawActions extends OMSRunner with OMSInitializer {

  def main(args: Array[String]): Unit = {
    logInfo(s"Starting processing the OMS Raw Data : $omsConfig")
    // Get the current version for Raw Actions
    val currentRawActionsVersion = getCurrentRawActionsVersion()
    // Get Last OMS Raw Actions Commit Version that was processed
    val lastProcessedRawActionsVersion = getLastProcessedRawActionsVersion()
    if (currentRawActionsVersion == 0 ||
      lastProcessedRawActionsVersion < currentRawActionsVersion) {
      // Read the changed data since that version
      val newRawActions = getUpdatedRawActions(lastProcessedRawActionsVersion)
      // Extract and Persist Commit Info from the new Raw Actions
      processCommitInfoFromRawActions(newRawActions)
      // Extract, Compute Version Snapshots and Persist Action Info from the new Raw Actions
      processActionSnapshotsFromRawActions(newRawActions)
      // Find the latest version from the newly processed raw actions
      val latestRawActionVersion = getLatestRawActionsVersion(newRawActions)
      // Update history with the version
      updateLastProcessedRawActions(latestRawActionVersion)
    }
  }
}
