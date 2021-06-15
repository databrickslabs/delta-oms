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
package io.delta.oms.common

import io.delta.oms.configuration.{ConfigurationSettings, EnvironmentResolver}
import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.delta.test.DeltaTestSharedSession
import org.apache.spark.sql.test.SharedSparkSession

class OMSInitializerSuite extends QueryTest with SharedSparkSession with DeltaTestSharedSession
  with BeforeAndAfter with ConfigurationSettings with OMSInitializer{
  import testImplicits._
  // scalastyle:on funsuite

  test("Inbuilt Configuration Settings") {
    assert(environmentConfigFile == "inbuilt")
    assert(environment == EnvironmentResolver.fetchEnvironment("inbuilt"))
    assert(omsConfig.dbName == "oms_default_inbuilt")
  }

  test("Initialize OMS Database and tables") {
    assert(!spark.catalog.databaseExists(omsConfig.dbName))
    initializeOMS(omsConfig, dropAndRecreate = true)
    assert(spark.catalog.databaseExists(omsConfig.dbName))
    assert(spark.catalog.tableExists(omsConfig.dbName, omsConfig.tableConfig))
    assert(spark.catalog.tableExists(omsConfig.dbName, omsConfig.pathConfigTable))
    assert(spark.catalog.tableExists(omsConfig.dbName, omsConfig.rawActionTable))
    assert(spark.catalog.tableExists(omsConfig.dbName, omsConfig.processedHistoryTable))
  }
}
