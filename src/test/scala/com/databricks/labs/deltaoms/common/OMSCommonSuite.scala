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

import com.databricks.labs.deltaoms.configuration.{ConfigurationSettings, OMSConfig}
import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.delta.test.DeltaTestSharedSession
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.QueryTest

class OMSCommonSuite extends QueryTest with SharedSparkSession with DeltaTestSharedSession
  with BeforeAndAfter with ConfigurationSettings {

  test("Command Line Parsing Valid Switches") {
    val args = Array("--skipPathConfig")
    assert(OMSCommandLineParser.parseCommandArgsAndConsolidateOMSConfig(args,
      OMSConfig()) == OMSConfig(skipPathConfig = true))
    assert(OMSCommandLineParser.parseCommandArgsAndConsolidateOMSConfig(args,
      omsConfig) == omsConfig.copy(skipPathConfig = true))
  }

  test("Command Line Parsing Invalid Switches") {
    val args = Array("--skipABC")
    assertThrows[scala.MatchError](OMSCommandLineParser
      .parseCommandArgsAndConsolidateOMSConfig(args, OMSConfig()))
  }

  test("Command Line Parsing Invalid key/value parameters") {
    val args = Array("--dbName=abc", "--baseCheckpointLocation=/checkBaseLocation")
    assertThrows[scala.MatchError](OMSCommandLineParser
      .parseCommandArgsAndConsolidateOMSConfig(args, OMSConfig()))
  }

  test("Command Line Parsing Valid Key/Value Parameters") {
    val args = Array("--dbName=abc", "--baseLocation=/sampleBase",
      "--checkpointBase=/checkBase", "--checkpointSuffix=_checkSuffix_123")
    assert(OMSCommandLineParser.parseCommandArgsAndConsolidateOMSConfig(args, OMSConfig()) ==
      OMSConfig(dbName = Some("abc"), baseLocation = Some("/sampleBase"),
        checkpointBase = Some("/checkBase"), checkpointSuffix = Some("_checkSuffix_123")))

    assert(OMSCommandLineParser.parseCommandArgsAndConsolidateOMSConfig(args, omsConfig) ==
      omsConfig.copy(dbName = Some("abc"), baseLocation = Some("/sampleBase"),
        checkpointBase = Some("/checkBase"), checkpointSuffix = Some("_checkSuffix_123")))
  }

  test("Unknown Command Line Argument") {
    val argsAll = Array("dbName=abc")
    val errorThrown = intercept[java.lang.RuntimeException](OMSCommandLineParser.
      consolidateAndValidateOMSConfig(argsAll, OMSConfig()))
    assert(errorThrown.getMessage.contains("Unknown OMS Command Line Options"))
  }

  test("Consolidate and Validate Command Line Parsing Valid Parameters") {
    val argsAll = Array("--dbName=abc", "--baseLocation=/sampleBase",
      "--checkpointBase=/checkBase", "--checkpointSuffix=_checkSuffix_123",
      "--skipPathConfig", "--skipInitializeOMS",
      "--skipWildcardPathsConsolidation", "--startingStream=1", "--endingStream=20")
    assert(OMSCommandLineParser.consolidateAndValidateOMSConfig(argsAll, OMSConfig()) ==
      OMSConfig(dbName = Some("abc"), baseLocation = Some("/sampleBase"),
        checkpointBase = Some("/checkBase"), checkpointSuffix = Some("_checkSuffix_123"),
        skipPathConfig = true, skipInitializeOMS = true, consolidateWildcardPaths = false,
        endingStream = 20))

    val argsOptional = Array("--baseLocation=/sampleBase",
      "--checkpointBase=/checkBase", "--checkpointSuffix=_checkSuffix_123")
    assert(OMSCommandLineParser.consolidateAndValidateOMSConfig(argsOptional, omsConfig) ==
      omsConfig.copy(dbName = Some("oms_default_inbuilt"), baseLocation = Some("/sampleBase"),
        checkpointBase = Some("/checkBase"), checkpointSuffix = Some("_checkSuffix_123")))
  }

  test("Consolidate and Validate Command Line Parsing InValid Parameters") {
    val missingDBNameArgs = Array("--baseLocation=/sampleBase",
      "--checkpointBase=/checkBase", "--checkpointSuffix=_checkSuffix_123")
    val missingDbNameException = intercept[java.lang.AssertionError]{
      OMSCommandLineParser.consolidateAndValidateOMSConfig(missingDBNameArgs, OMSConfig())
    }
    assert(missingDbNameException.getMessage
      .contains("Mandatory configuration OMS DB Name missing"))

    val missingBaseLocationArgs = Array("--dbName=abc",
      "--checkpointBase=/checkBase", "--checkpointSuffix=_checkSuffix_123")
    val missingBaseLocationMissingException = intercept[java.lang.AssertionError]{
      OMSCommandLineParser.consolidateAndValidateOMSConfig(missingBaseLocationArgs, OMSConfig())
    }
    assert(missingBaseLocationMissingException.getMessage
      .contains("Mandatory configuration OMS Base Location missing"))

    val missingCheckpointBaseArgs = Array("--dbName=abc", "--baseLocation=/sampleBase",
      "--checkpointSuffix=_checkSuffix_123")
    val missingCheckpointBaseException = intercept[java.lang.AssertionError]{
      OMSCommandLineParser.consolidateAndValidateOMSConfig(missingCheckpointBaseArgs, OMSConfig(),
        isBatch = false)
    }
    assert(missingCheckpointBaseException.getMessage
      .contains("Mandatory configuration OMS Checkpoint Base Location missing"))

    val missingCheckpointSuffixArgs = Array("--dbName=abc", "--baseLocation=/sampleBase",
      "--checkpointBase=/checkBase")
    val missingCheckpointSuffixException = intercept[java.lang.AssertionError]{
      OMSCommandLineParser
        .consolidateAndValidateOMSConfig(missingCheckpointSuffixArgs, OMSConfig(), isBatch = false)
    }
    assert(missingCheckpointSuffixException.getMessage
      .contains("Mandatory configuration OMS Checkpoint Suffix missing."))
  }

  test("Command Line Parameters not passed") {
    val argsEmpty = Array.empty[String]
    assert(OMSCommandLineParser.consolidateAndValidateOMSConfig(argsEmpty, omsConfig) == omsConfig)
  }

}
