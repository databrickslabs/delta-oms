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

package com.databricks.labs.deltaoms.common

import org.apache.spark.internal.Logging
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent,
  QueryStartedEvent, QueryTerminatedEvent}

class OMSStreamingQueryListener extends StreamingQueryListener with Logging {

      override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
        logInfo(s"Query=${queryStarted.name}:QueryId=${queryStarted.id}:STARTED:" +
          s"RunId=${queryStarted.runId}:Timestamp=${queryStarted.timestamp}" )
      }
      override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
        logInfo(s"QueryId=${queryTerminated.id}:RunId=${queryTerminated.runId}:TERMINATED:" +
          s"Exception: ${queryTerminated.exception.toString}"
        )
      }
      override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
        if (queryProgress.progress.numInputRows > 0) {
          logInfo(s"Query Progress: ${queryProgress.progress.prettyJson}")
        }
      }
}
