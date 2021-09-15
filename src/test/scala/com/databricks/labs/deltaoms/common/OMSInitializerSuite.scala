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

import com.databricks.labs.deltaoms.configuration.{ConfigurationSettings, EnvironmentResolver, Local, OMSConfig, Remote}
import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.delta.test.DeltaTestSharedSession
import org.apache.spark.sql.test.SharedSparkSession

class OMSInitializerSuite extends QueryTest with SharedSparkSession with DeltaTestSharedSession
  with BeforeAndAfter with ConfigurationSettings with OMSInitializer{
  import testImplicits._
  // scalastyle:on funsuite

  test("Validate empty Configuration Settings") {
    System.setProperty("OMS_CONFIG_FILE", "empty")
    assert(environmentConfigFile == "empty")
    assert(environment == EnvironmentResolver.fetchEnvironment("empty"))
    assert(omsConfig.dbName.isEmpty)
    System.clearProperty("OMS_CONFIG_FILE")
  }

  test("Initialize from Local File") {
    val testConfigFile = getClass.getResource("/test_reference.conf").getPath
    System.setProperty("OMS_CONFIG_FILE", "file://" + testConfigFile)
    assert(environment == Local)
    assert(omsConfig.dbName.get.contains("FORTESTING"))
  }

  test("Initialize from Remote File") {
    val testConfigFile = getClass.getResource("/test_reference.conf").getPath
    System.setProperty("OMS_CONFIG_FILE", testConfigFile)
    assert(environment == Remote)
    assert(omsConfig.dbName.get.contains("FORTESTING"))
  }

  test("Initialization using wrong config file") {
    val testConfigFile = getClass.getResource("/test_reference.conf").getPath + "a"
    assertThrows[java.lang.RuntimeException](fetchConfigFileContent(testConfigFile))
  }

  test("Inbuilt Configuration Settings") {
    System.setProperty("OMS_CONFIG_FILE", "inbuilt")
    assert(environmentConfigFile == "inbuilt")
    assert(environment == EnvironmentResolver.fetchEnvironment("inbuilt"))
    assert(omsConfig.dbName.get == "oms_default_inbuilt")
  }

  test("Initialize OMS Database and tables") {
    val dbName = omsConfig.dbName.get
    assert(!spark.catalog.databaseExists(dbName))
    initializeOMS(omsConfig)
    assert(spark.catalog.databaseExists(dbName))
    initializeOMS(omsConfig, dropAndRecreate = true)
    assert(spark.catalog.databaseExists(dbName))
    assert(spark.catalog.tableExists(dbName, omsConfig.sourceConfigTable))
    assert(spark.catalog.tableExists(dbName, omsConfig.pathConfigTable))
    assert(spark.catalog.tableExists(dbName, omsConfig.rawActionTable))
    assert(spark.catalog.tableExists(dbName, omsConfig.processedHistoryTable))
  }

  test("cleanupOMS DB Path Exception") {
    val dbInvalidPathOMSConfig =
      OMSConfig(dbName = Some("abc"), baseLocation = Some("s3://sampleBase"))
    assertThrows[java.io.IOException](cleanupOMS(dbInvalidPathOMSConfig))
  }
}
