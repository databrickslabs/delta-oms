package com.databricks.labs.deltaoms.utils

import java.net.URI

import com.databricks.labs.deltaoms.model.{DatabaseDefinition, TableDefinition}
import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.delta.{ DeltaTableIdentifier, DeltaTableUtils}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.util.SerializableConfiguration

import scala.util.{Failure, Success, Try}

trait UtilityOperations extends Serializable with Logging {

  def fetchMetaStoreDeltaTables(databases: Option[String], pattern: Option[String]) = {
    val srcDatabases = if(databases.isDefined && databases.get.trim.nonEmpty) {
      databases.get.trim.split("[,;:]").map(_.trim).toSeq
    } else {
      Seq.empty[String]
    }
    val tablePattern: String = pattern.map{_.trim}.filterNot{_.isEmpty}.getOrElse("*")
    getDeltaTablesFromMetastore(srcDatabases, tablePattern)
  }

  def getDeltaTablesFromMetastore(databases: Seq[String] = Seq.empty[String],
                                  pattern: String = "*"): Seq[DeltaTableIdentifier] = {
    val spark = SparkSession.active
    val sessionCatalog = spark.sessionState.catalog
    val databaseList = if(databases.nonEmpty)
      sessionCatalog.listDatabases.filter(databases.contains(_))
    else
      sessionCatalog.listDatabases
    val allTables = databaseList.flatMap(dbName =>
      sessionCatalog.listTables(dbName,pattern,includeLocalTempViews = false))
    allTables.flatMap(tableIdentifierToDeltaTableIdentifier)
  }

  def tableIdentifierToDeltaTableIdentifier(identifier: TableIdentifier): Option[DeltaTableIdentifier] = {
    val spark = SparkSession.active
    val dTableIdTry = Try {
      DeltaTableIdentifier(spark, identifier)
    }
    dTableIdTry match {
      case Success(deltaID) => deltaID
      case Failure(exception) => {
        logError(s"Error while accessing table $identifier : $exception")
        None
      }
    }
  }

  def validateDeltaLocation(locationId: String) = {
    val spark = SparkSession.active
    val sessionCatalog = spark.sessionState.catalog

    if(sessionCatalog.databaseExists(locationId)){
      val dbTables = sessionCatalog.listTables(locationId,"*",includeLocalTempViews = false)
      val dbDeltaTableIds = dbTables.flatMap(tableIdentifierToDeltaTableIdentifier)
      dbDeltaTableIds.map(ddt => (Some(ddt.unquotedString), ddt.getPath(spark).toString))
    } else {
      val pathDTableTry = Try { DeltaTable.forPath(spark, locationId) }
      pathDTableTry match {
        case Success(_) => Seq((None, locationId))
        case Failure(e) => {
          val nameDTableTry = Try {
            DeltaTable.forName(spark, locationId)
          }
          nameDTableTry match {
            case Success(_) => {
              val tableId = spark.sessionState.sqlParser.parseTableIdentifier(locationId)
              Seq((Some(locationId),
                new Path(spark.sessionState.catalog.getTableMetadata(tableId).location).toString))
            }
            case Failure(ex) => {
              logError(s"Error while accessing Delta location $locationId." +
                s"It should be a valid database, path or fully qualified table name.\n " +
                s"Exception thrown: $ex")
              throw ex
            }
          }
        }
      }
    }
  }

  def getDeltaPathFromDelTableIdentifiers(deltaTableIds: Seq[DeltaTableIdentifier]): Seq[Path] = {
    val spark = SparkSession.active
    deltaTableIds.map(_.getPath(spark))
  }

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
      val dataFrameCreateWriter = emptyDF
        .writeTo(fqTableName)
        .tableProperty("location",tableDefn.path)
      val dataFrameCreateWriterWithComment = tableDefn.comment.foldLeft(dataFrameCreateWriter){
        (dfw, c) => dfw.tableProperty("comment",c)
      }
      val dataFrameCreateWriterWithProperties = tableDefn.properties.toList
        .foldLeft(dataFrameCreateWriterWithComment){
          (dfw, kv) => dfw.tableProperty(kv._1,kv._2)
      }
      if (tableDefn.partitionColumnNames.nonEmpty) {
        val partitionColumns = tableDefn.partitionColumnNames.map(col)
        dataFrameCreateWriterWithProperties
          .using("delta")
          .partitionedBy(partitionColumns.head, partitionColumns.tail: _*)
          .createOrReplace()
      } else {
        dataFrameCreateWriterWithProperties
          .using("delta")
          .createOrReplace()
      }
    }
  }

  def deleteDirectory(dirName: String) = {
    val fileSystem = FileSystem.get(new URI(dirName),
      SparkSession.active.sparkContext.hadoopConfiguration)
    fileSystem.delete(new Path(dirName), true)
  }

  def dropDatabase(dbName: String) = {
    SparkSession.active.sql(s"DROP DATABASE IF EXISTS $dbName CASCADE")
  }

  def getFileModificationTimeUDF() = {
    val spark = SparkSession.active
    val conf = spark.sparkContext.broadcast(
      new SerializableConfiguration(spark.sessionState.newHadoopConf()))

    udf((filePath: String) => {
      val p = new Path(filePath)
      p.getFileSystem(conf.value.value).listStatus(p).map(_.getModificationTime).head/1000
    })
  }

  def getDeltaWildCardPathUDF() = {
    udf((filePath: String) => {
      filePath.split("/").zipWithIndex
        .map{ case (v, i) => if(i < 3) v else "*" }.mkString("/")+"/_delta_log/*.json"
    })
  }

}

object UtilityOperations extends UtilityOperations
