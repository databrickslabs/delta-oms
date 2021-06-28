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

package io.delta.oms.configuration

import org.apache.spark.sql.SparkSession

trait SparkSettings extends Serializable with ConfigurationSettings {
  protected val sparkSession: SparkSession = environment match {
    case InBuilt => SparkSession.builder()
      .master("local")
      .config("spark.driver.host", "localhost")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .appName("DELTA_OMS_INBUILT").getOrCreate()
    case Local => SparkSession.builder()
      .master("local[*]")
      .config("spark.driver.host", "localhost")
      .config("spark.sql.warehouse.dir", omsConfig.baseLocation.get)
      .appName("DELTA_OMS_LOCAL")
      .enableHiveSupport()
      .getOrCreate()
    case _ => val spark = SparkSession.builder().appName("Delta OMS").getOrCreate()
      spark.conf.set("spark.databricks.delta.properties.defaults.enableChangeDataFeed",
        value = true)
      spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", value = true)
      spark.conf.set("spark.databricks.delta.autoCompact.enabled", value = true)
      spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", value = true)
      spark
  }

  def spark: SparkSession = SparkSession.active

}
