package io.delta.oms.common

import io.delta.oms.configuration.ConfigurationSettings
import io.delta.oms.model.OMSCommandLineArgs
import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.delta.test.DeltaTestSharedSession
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.QueryTest

class OMSCommonSuite extends QueryTest with SharedSparkSession with DeltaTestSharedSession
  with BeforeAndAfter with ConfigurationSettings {

  test("Command Line Parsing Valid Switches") {
    val args = Array("--skipPathConfig")
    assert(OMSCommandLineParser.parseOMSCommandArgs(args) == OMSCommandLineArgs(true))
  }

  test("Command Line Parsing Invalid Switches") {
    val args = Array("--skipABC")
    assertThrows[scala.MatchError](OMSCommandLineParser.parseOMSCommandArgs(args))
  }

  test("Command Line Parsing Invalid key/value parameters") {
    val args = Array("--dbName=abc", "--baseCheckpointLocation=/checkBaseLocation")
    assertThrows[scala.MatchError](OMSCommandLineParser.parseOMSCommandArgs(args))
  }

  test("Command Line Parsing Valid Key/Value Parameters") {
    val args = Array("--dbName=abc", "--baseLocation=/sampleBase",
      "--checkpointBase=/checkBase", "--checkpointSuffix=_checkSuffix_123")
    assert(OMSCommandLineParser.parseOMSCommandArgs(args) ==
      OMSCommandLineArgs(dbName = Some("abc"), baseLocation = Some("/sampleBase"),
        checkpointBase = Some("/checkBase"), checkpointSuffix = Some("_checkSuffix_123")))
  }

}
