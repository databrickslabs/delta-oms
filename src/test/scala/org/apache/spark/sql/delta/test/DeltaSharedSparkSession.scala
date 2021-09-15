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

package org.apache.spark.sql.delta.test

import io.delta.sql.DeltaSparkSessionExtension

import org.apache.spark.sql.test.{SharedSparkSession, TestSparkSession}
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.internal.SQLConf

class DeltaSharedSparkSession(sparkConf: SparkConf) extends TestSparkSession(sparkConf) {
  override val extensions: SparkSessionExtensions = {
    val extensions = new SparkSessionExtensions
    new DeltaSparkSessionExtension().apply(extensions)
    extensions
  }
}

trait DeltaTestSharedSession { self: SharedSparkSession =>
    override protected def createSparkSession: TestSparkSession = {
      SparkSession.cleanupAnyExistingSession()
      val session = new DeltaSharedSparkSession(sparkConf)
      session.conf.set(SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key, classOf[DeltaCatalog].getName)
      // session.conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      session
    }
}

