package com.databricks.labs.deltaods.utils

import com.databricks.labs.deltaods.model.{DatabaseDefinition, TableDefinition}
import io.delta.tables.DeltaTable
import org.apache.spark.internal.Logging
import org.apache.spark.sql.delta.DeltaTableUtils
import org.apache.spark.sql.{Row, SparkSession}

import scala.util.{Failure, Success, Try}

trait UtilityOperations extends Serializable with Logging {

  def createDatabaseIfAbsent(dbDefn: DatabaseDefinition): Unit = {
    val spark = SparkSession.active
    val dBCreateSQL = new StringBuilder(s"CREATE DATABASE IF NOT EXISTS ${dbDefn.databaseName} ")
    if (dbDefn.comment.nonEmpty) {
      dBCreateSQL.append(s"COMMENT '${dbDefn.comment.get}' ")
    }
    if (dbDefn.location.nonEmpty) {
      dBCreateSQL.append(s"LOCATION '${dbDefn.location.get}' ")
    }
    if (dbDefn.properties.nonEmpty) {
      val tableProperties = dbDefn.properties.map(_.productIterator.mkString("'", "'='", "'")).mkString(",")
      dBCreateSQL.append(s"WITH DBPROPERTIES($tableProperties) ")
    }
    logDebug(s"CREATING DATABASE using SQL => ${dBCreateSQL.toString()}")
    Try {
      spark.sql(dBCreateSQL.toString())
    } match {
      case Success(value) => logInfo(s"Successfully created the database ${dbDefn.databaseName}")
      case Failure(exception) => throw new RuntimeException(s"Unable to create the Database: $exception")
    }
  }

  def createTableIfAbsent(tableDefn: TableDefinition): Unit = {
    val spark = SparkSession.active
    val fqTableName = s"${tableDefn.databaseName}.${tableDefn.tableName}"
    val tableId = spark.sessionState.sqlParser.parseTableIdentifier(s"${fqTableName}")
    if (!DeltaTable.isDeltaTable(tableDefn.path) && !DeltaTableUtils.isDeltaTable(spark, tableId)) {
      val emptyDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], tableDefn.schema)
      if (tableDefn.partitionColumnNames.nonEmpty) {
        emptyDF
          .write
          .partitionBy(tableDefn.partitionColumnNames: _*)
          .format("delta")
          .mode("overwrite")
          .save(tableDefn.path)
      } else {
        emptyDF.write.format("delta").mode("overwrite").save(tableDefn.path)
      }

      val tableCreateSQL = new StringBuilder(s"CREATE TABLE IF NOT EXISTS ${fqTableName} USING DELTA ")
      /*if(tableDefn.partitionColumnNames.nonEmpty){
        tableCreateSQL.append(s"PARTITIONED BY (${tableDefn.partitionColumnNames.mkString(",")}) ")
      }*/
      if (tableDefn.comment.nonEmpty) {
        tableCreateSQL.append(s"COMMENT '${tableDefn.comment.get}' ")
      }
      if (tableDefn.path.nonEmpty) {
        tableCreateSQL.append(s"LOCATION '${tableDefn.path}' ")
      }
      if (tableDefn.properties.nonEmpty) {
        val tableProperties = tableDefn.properties.map(_.productIterator.mkString("'", "'='", "'")).mkString(",")
        tableCreateSQL.append(s"TBLPROPERTIES ($tableProperties) ")
      }
      logDebug(s"CREATING TABLE using SQL => ${tableCreateSQL.toString()}")
      Try {
        spark.sql(tableCreateSQL.toString())
      } match {
        case Success(value) => logInfo(s"Successfully created the table ${fqTableName}")
        case Failure(exception) => throw new RuntimeException(s"Unable to create the table" +
          s" ${fqTableName}: $exception")
      }
    }
  }
}

object UtilityOperations extends UtilityOperations
