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

import com.databricks.labs.deltaoms.configuration.{OMSConfig, SparkSettings}

trait OMSSparkConf extends Serializable with SparkSettings {

  def buildConfKey(key: String): String = s"databricks.labs.deltaoms.${key}"

  val BASE_LOCATION = buildConfKey("base.location")
  val DB_NAME = buildConfKey("db.name")
  val CHECKPOINT_BASE = buildConfKey("checkpoint.base")
  val CHECKPOINT_SUFFIX = buildConfKey("checkpoint.suffix")

  def consolidateOMSConfigFromSparkConf(config: OMSConfig): OMSConfig = {
    Seq(BASE_LOCATION, DB_NAME, CHECKPOINT_BASE, CHECKPOINT_SUFFIX).foldLeft(config) {
      (omsSparkConfig, configValue) => {
        configValue match {
          case BASE_LOCATION => spark.conf.getOption(BASE_LOCATION).fold(omsSparkConfig) {
            sprkVal => omsSparkConfig.copy(baseLocation = Some(sprkVal))
          }
          case DB_NAME => spark.conf.getOption(DB_NAME).fold(omsSparkConfig) {
            sprkVal => omsSparkConfig.copy(dbName = Some(sprkVal))
          }
          case CHECKPOINT_BASE => spark.conf.getOption(CHECKPOINT_BASE).fold(omsSparkConfig) {
            sprkVal => omsSparkConfig.copy(checkpointBase = Some(sprkVal))
          }
          case CHECKPOINT_SUFFIX => spark.conf.getOption(CHECKPOINT_SUFFIX).fold(omsSparkConfig) {
            sprkVal => omsSparkConfig.copy(checkpointSuffix = Some(sprkVal))
          }
        }
      }
    }
  }
}

object OMSSparkConf extends OMSSparkConf
