package com.databricks.labs.deltaods.common

import com.databricks.labs.deltaods.model.TableIdentifierWithLatestVersion

import scala.util.{Failure, Success, Try}

trait ODSOperations extends Serializable with SparkSettings {

  import implicits._

  val implicits = spark.implicits

  def fetchLastUpdatedVersions(fqdt: List[(String, String)]): Seq[TableIdentifierWithLatestVersion] = {

    val tableIds = fqdt.map(t => TableIdentifierWithLatestVersion(t._2, t._1, 0))
    val tableIdsDf = tableIds.toDF
    tableIdsDf.show()
    println(s"****** ${odsConfig.dbName}.${odsConfig.latestVersionTable}")
    spark.sql(s"show tables in ${odsConfig.dbName}").show()
    val odsTableLastVersions = Try {
      spark.table(s"${odsConfig.dbName}.${odsConfig.latestVersionTable}")
    }
    odsTableLastVersions match {
      case Success(i) => {
        val odsLastVersionDF = i
        if (odsLastVersionDF.isEmpty) {
          tableIdsDf.write.format("delta")
            .mode("overwrite")
            .option("path", s"${odsConfig.baseLocation}/${odsConfig.latestVersionTable}/")
            .saveAsTable(s"${odsConfig.dbName}.${odsConfig.latestVersionTable}")
          tableIds
        } else {
          odsLastVersionDF.join(tableIdsDf, Seq("tableName", "databaseName"))
            .select(odsLastVersionDF("tableName"), odsLastVersionDF("databaseName"),
              odsLastVersionDF("version"), tableIdsDf("refreshTime"))
            .as[TableIdentifierWithLatestVersion].collect.toSeq
        }
      }
      case Failure(s) => {
        logInfo(s"Reading ODS last version table failed. Reason: $s")
        throw new RuntimeException(s)
      }
    }
  }
}
