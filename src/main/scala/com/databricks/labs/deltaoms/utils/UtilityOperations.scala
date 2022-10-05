/*
 * Copyright (2021) Databricks, Inc.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE
 * AND NONINFRINGEMENT.
 *
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR
 * THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 * See the Full License for the specific language governing permissions and
 * limitations under the License.
 */

package com.databricks.labs.deltaoms.utils

import java.net.URI

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}
import com.databricks.labs.deltaoms.model.{DatabaseDefinition, SourceConfig, TableDefinition}
import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.{FileSystem, Path, RemoteIterator}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.delta.{DeltaTableIdentifier, DeltaTableUtils}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.util.SerializableConfiguration

trait UtilityOperations extends Serializable with Logging {

  def fetchMetaStoreDeltaTables(databases: Option[String], pattern: Option[String])
  : Seq[DeltaTableIdentifier] = {
    val srcDatabases = if (databases.isDefined && databases.get.trim.nonEmpty) {
      databases.get.trim.split("[,;:]").map(_.trim).toSeq
    } else {
      Seq.empty[String]
    }
    val tablePattern: String = pattern.map {
      _.trim
    }.filterNot {
      _.isEmpty
    }.getOrElse("*")
    getDeltaTablesFromMetastore(srcDatabases, tablePattern)
  }

  def getDeltaTablesFromMetastore(databases: Seq[String] = Seq.empty[String],
    pattern: String = "*"): Seq[DeltaTableIdentifier] = {
    val spark = SparkSession.active
    val sessionCatalog = spark.sessionState.catalog
    val databaseList = if (databases.nonEmpty) {
      sessionCatalog.listDatabases.filter(databases.contains(_))
    } else {
      sessionCatalog.listDatabases
    }
    val allTables = databaseList.flatMap(dbName =>
      sessionCatalog.listTables(dbName, pattern, includeLocalTempViews = false))
    allTables.flatMap(tableIdentifierToDeltaTableIdentifier)
  }

  def validateDeltaLocation(sourceConfig: SourceConfig): Seq[(Option[String], String,
    Map[String, String])] = {
    val spark = SparkSession.active
    val sessionCatalog = spark.sessionState.catalog

    if (!sourceConfig.path.contains("/") && !sourceConfig.path.contains(".")
      && sessionCatalog.databaseExists(sourceConfig.path)) {
      val dbTables = sessionCatalog.listTables(sourceConfig.path, "*",
        includeLocalTempViews = false)
      val dbDeltaTableIds = dbTables.flatMap(tableIdentifierToDeltaTableIdentifier)
      dbDeltaTableIds.map(ddt => (Some(ddt.unquotedString), ddt.getPath(spark).toString,
        sourceConfig.parameters))
    } else {
      val pathDTableTry = Try {
        DeltaTable.forPath(spark, sourceConfig.path)
      }
      pathDTableTry match {
        case Success(_) => Seq((None, sourceConfig.path, sourceConfig.parameters))
        case Failure(e) =>
          val nameDTableTry = Try {
            DeltaTable.forName(spark, sourceConfig.path)
          }
          nameDTableTry match {
            case Success(_) =>
              val tableId = spark.sessionState.sqlParser.parseTableIdentifier(sourceConfig.path)
              Seq((Some(sourceConfig.path),
                new Path(spark.sessionState.catalog.getTableMetadata(tableId).location).toString,
                sourceConfig.parameters))
            case Failure(ex) =>
              logError(s"Error while accessing Delta location $sourceConfig." +
                s"It should be a valid database, table path or fully qualified table name.\n " +
                s"Exception thrown: $ex")
              throw ex
          }
      }
    }
  }

  def tableIdentifierToDeltaTableIdentifier(identifier: TableIdentifier)
  : Option[DeltaTableIdentifier] = {
    val spark = SparkSession.active
    DeltaTableIdentifier(spark, identifier)
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
      val tableProperties = dbDefn.properties.map(_.productIterator.mkString("'", "'='", "'"))
        .mkString(",")
      dBCreateSQL.append(s"WITH DBPROPERTIES($tableProperties) ")
    }
    logDebug(s"CREATING DATABASE using SQL => ${dBCreateSQL.toString()}")
    Try {
      spark.sql(dBCreateSQL.toString())
    } match {
      case Success(value) => logInfo(s"Successfully created the database ${dbDefn.databaseName}")
      case Failure(exception) =>
        throw new RuntimeException(s"Unable to create the Database: $exception")
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
        .tableProperty("location", tableDefn.path)
      val dataFrameCreateWriterWithComment = tableDefn.comment.foldLeft(dataFrameCreateWriter) {
        (dfw, c) => dfw.tableProperty("comment", c)
      }
      val dataFrameCreateWriterWithProperties = tableDefn.properties.toList
        .foldLeft(dataFrameCreateWriterWithComment) {
          (dfw, kv) => dfw.tableProperty(kv._1, kv._2)
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

  def deleteDirectory(dirName: String): Boolean = {
    val fileSystem = FileSystem.get(new URI(dirName),
      SparkSession.active.sparkContext.hadoopConfiguration)
    fileSystem.delete(new Path(dirName), true)
  }

  def dropDatabase(dbName: String): DataFrame = {
    SparkSession.active.sql(s"DROP DATABASE IF EXISTS $dbName CASCADE")
  }

  def resolveWildCardPath(filePath: String, wildCardLevel: Int) : String = {
    assert(wildCardLevel == -1 || wildCardLevel == 0 || wildCardLevel == 1,
      "WildCard Level should be -1, 0 or 1")
    val modifiedPath = if (wildCardLevel == 0) {
      (filePath.split("/").dropRight(1):+"*")
    } else if (wildCardLevel == 1) {
      (filePath.split("/").dropRight(2):+"*":+"*")
    } else {
      filePath.split("/")
    }
    modifiedPath.mkString("/") + "/_delta_log/*.json"
  }

  def getDeltaWildCardPathUDF(): UserDefinedFunction = {
    udf((filePath: String, wildCardLevel: Int) => resolveWildCardPath(filePath, wildCardLevel))
  }

  def consolidateWildCardPaths(wildCardPaths: Seq[(String, String)]): Seq[(String, String)] = {
    wildCardPaths.foldLeft(ListBuffer.empty[(String, String)]) { (l, a) =>
      if (l.nonEmpty) {
        val split_a = a._1.split("\\*")(0)
        for (b <- l.toList) {
          val split_b = b._1.split("\\*")(0)
          if (split_b contains split_a) {
            l -= b
            if (! l.contains(a)) {
              l += a
            }
          } else {
            if (!(split_a contains split_b) && ! l.contains(a)) {
              l += a
            }
          }
        }
        l
      } else {
        l += a
      }
    }.toList
  }

  def listSubDirectories(sourceConfig: SourceConfig, conf: SerializableConfiguration):
  Array[SourceConfig] = {
    val skipProcessing = sourceConfig.skipProcessing
    val parameters = sourceConfig.parameters
    val subDirectories = listSubDirectories(sourceConfig.path, conf)
    subDirectories.map(d => SourceConfig(d, skipProcessing, parameters))
  }

  def listSubDirectories(path: String, conf: SerializableConfiguration): Array[String] = {
    val fs = new Path(path).getFileSystem(conf.value)
    fs.listStatus(new Path(path)).filter(_.isDirectory).map(_.getPath.toString)
  }

  def recursiveListDeltaTablePaths(sourceConfig: SourceConfig, conf: SerializableConfiguration):
  Set[SourceConfig] = {
    val skipProcessing = sourceConfig.skipProcessing
    val parameters = sourceConfig.parameters
    recursiveListDeltaTablePaths(sourceConfig.path, conf)
      .map(d => SourceConfig(d, skipProcessing, parameters))
  }

  def recursiveListDeltaTablePaths(path: String, conf: SerializableConfiguration): Set[String] = {
    implicit def remoteIteratorToIterator[A](ri: RemoteIterator[A]): Iterator[A] =
      new Iterator[A] {
        override def hasNext: Boolean = ri.hasNext
        override def next(): A = ri.next()
    }
    val fs = new Path(path).getFileSystem(conf.value)
    fs.listFiles(new Path(path), true)
      .map(_.getPath.toString)
      .filter(x => x.contains("_delta_log") && x.endsWith(".json"))
      .map(_.split("/").dropRight(2).mkString("/"))
      .toSet
  }
}
object UtilityOperations extends UtilityOperations
