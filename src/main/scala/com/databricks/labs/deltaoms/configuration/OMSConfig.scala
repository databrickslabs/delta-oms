/*
 * Copyright (2021) Databricks, Inc.
 *
 * Delta Operational Metrics Store(DeltaOMS)
 *
 * Copyright 2021 Databricks, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.databricks.labs.deltaoms.configuration

case class OMSConfig(baseLocation: Option[String] = None,
  dbName: Option[String] = None,
  checkpointBase: Option[String] = None,
  checkpointSuffix: Option[String] = None,
  rawActionTable: String = "rawactions",
  sourceConfigTable: String = "sourceconfig",
  pathConfigTable: String = "pathconfig",
  processedHistoryTable: String = "processedhistory",
  commitInfoSnapshotTable: String = "commitinfosnapshots",
  actionSnapshotTable: String = "actionsnapshots",
  consolidateWildcardPaths: Boolean = true,
  truncatePathConfig: Boolean = false,
  skipPathConfig: Boolean = false,
  skipInitializeOMS: Boolean = false,
  srcDatabases: Option[String] = None,
  tablePattern: Option[String] = None,
  triggerInterval: Option[String] = None,
  startingStream: Int = 1,
  endingStream: Int = 50)
