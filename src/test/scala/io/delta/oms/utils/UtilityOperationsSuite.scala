package io.delta.oms.utils

import io.delta.oms.common.OMSInitializer
import io.delta.oms.configuration.ConfigurationSettings
import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.delta.test.DeltaTestSharedSession
import org.apache.spark.sql.test.SharedSparkSession

class UtilityOperationsSuite extends QueryTest with SharedSparkSession with DeltaTestSharedSession
  with BeforeAndAfter with ConfigurationSettings with OMSInitializer {
  import testImplicits._
  // scalastyle:on funsuite

  test("Consolidate WildcardPaths") {
    val wcPaths1 = Array(("file:/tmp/oms/*/*/_delta_log/*.json", "abcd"),
      ("file:/tmp/oms/test_db_jun16/*/_delta_log/*.json", "efgh"))
    assert(UtilityOperations.consolidateWildCardPaths(wcPaths1).size == 1)

    val wcPaths2 = Array(
      ("file:/home/user/oms/test_db_jun16/*/_delta_log/*.json", "efgh"),
      ("file:/home/user/oms/*/*/_delta_log/*.json", "abcd"))
    assert(UtilityOperations.consolidateWildCardPaths(wcPaths1).size == 1)
  }
}
