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
import com.databricks.labs.deltaoms.model._
import org.apache.hadoop.fs.{FileSystem, Path, RemoteIterator}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.util.SerializableConfiguration

trait UtilityOperations extends Serializable with Logging {

  def resolveDeltaLocation(sourceConfig: SourceConfig):
  Array[(String, Option[String], Map[String, String])] = {
    val spark = SparkSession.active
    import spark.implicits._

    if (!sourceConfig.path.contains("/") && !sourceConfig.path.contains(".")) {
      // CATALOG
      if (sourceConfig.path.equalsIgnoreCase("hive_metastore")) {
        throw new RuntimeException(s"${sourceConfig.path} catalog is not supported. " +
          s"Configure databases from ${sourceConfig.path} catalog instead")
      }
      val tableList = spark.sql(s"SELECT (table_catalog || '.`' || " +
        s"table_schema || '`.`' || table_name || '`' ) as qualifiedName " +
        s"FROM ${sourceConfig.path}.information_schema.tables " +
        s"WHERE table_schema <> 'information_schema' " +
        s"AND table_type <> 'VIEW' " +
        s"AND (data_source_format = 'DELTA' OR data_source_format = 'delta')").as[String].collect

      tableList.map { tl =>
        getTableLocation(tl) match {
          case (t, l) => (t, l, Map("wildCardLevel" -> "1"))
        }
      }
    } else if (!sourceConfig.path.contains("/")
      && sourceConfig.path.count(_ == '.') == 1) {
      // SCHEMA
      val catalogName = sourceConfig.path.split('.')(0)
      val schemaName = sourceConfig.path.split('.')(1)

      val tableList = if (catalogName.equalsIgnoreCase("hive_metastore")) {
        val qualifiedSchemaName = if (isUCEnabled) sourceConfig.path else schemaName
        val qualifiedDBCol = if (isUCEnabled) col("database") else col("namespace")
        val qualifiedCatalogName = if (isUCEnabled) lit("`hive_metastore`.`") else lit("`")
        spark.sql(s"show tables in ${qualifiedSchemaName}")
          .filter("isTemporary <> true")
          .select(concat(qualifiedCatalogName, qualifiedDBCol,
            lit("`.`"), col("tableName"),
            lit("`")).as("qualifiedName")).as[String].collect()
      } else {
        spark.sql(s"SELECT (table_catalog || '.`' " +
          s"|| table_schema || '`.`' || table_name || '`' ) as qualifiedName " +
          s"FROM ${catalogName}.information_schema.tables " +
          s"WHERE table_schema = '${schemaName}' AND table_type <> 'VIEW' " +
          s"AND (data_source_format = 'DELTA' OR data_source_format = 'delta')").as[String].collect
      }

      tableList.map { tl =>
        getTableLocation(tl) match {
          case (t, l) => (t, l, Map("wildCardLevel" -> "0"))
        }
      }
    } else if (!sourceConfig.path.contains("/")
      && sourceConfig.path.count(_ == '.') == 2) {
      val catalogName = sourceConfig.path.split('.')(0)
      val tableName = if (catalogName.equalsIgnoreCase("hive_metastore") && !isUCEnabled) {
        s"`${sourceConfig.path.split('.')(1)}`.`${sourceConfig.path.split('.')(2)}`"
      } else sourceConfig.path
      // TABLE
      Array(getTableLocation(tableName)
      match { case (t, l) => (t, l, Map("wildCardLevel" -> "-1"))
      })
    } else if (sourceConfig.path.contains("/")) {
      Array((sourceConfig.path, Option(sourceConfig.path), Map("wildCardLevel" -> "0")))
    } else {
      throw new RuntimeException("Source Path should be a valid Catalog, " +
        "Schema ,table or Wildcard Path (/**)")
    }
  }

  def getTableLocation(tableName: String): (String, Option[String]) = {
    val spark = SparkSession.active
    import spark.implicits._

    val tableLocationTry = Try {
      spark.sql(s"DESCRIBE EXTENDED $tableName")
        .where("col_name in ('Location','Provider')")
        .drop("comment")
        .withColumn("id", lit(1))
        .groupBy("id")
        .pivot("col_name", Seq("Location", "Provider"))
        .agg(first("data_type"))
        .where(lower(col("Provider")) === "delta")
        .select("Location").as[String].collect()(0)
    }

    tableLocationTry match {
      case Success(loc) => (tableName, Some(loc))
      case Failure(ex) => logError(s"Error while validating location for $tableName : $ex")
        (tableName, None)
    }
  }

  def isUCEnabled: Boolean = {
    SparkSession.active.conf.getOption("spark.databricks.labs.deltaoms.ucenabled")
      .fold(true)(_.toBoolean)
  }

  def executeSQL(stmt: String, ctx: String): Unit = {
    val spark = SparkSession.active
    logInfo(s"CREATING $ctx using SQL => $stmt")
    Try {
      spark.sql(stmt)
    } match {
      case Success(value) => logInfo(s"Successfully created $ctx using SQL $stmt")
      case Failure(exception) =>
        throw new RuntimeException(s"Unable to create $ctx using SQL $stmt: $exception")
    }
  }

  def catalogCreationQuery(catalogDef: CatalogDefinition): (String, String) = {
    val catSql = new StringBuilder(s"CREATE CATALOG IF NOT EXISTS `${catalogDef.catalogName}` " +
      s"MANAGED LOCATION '${catalogDef.locationUrl.get}' ")
    if (catalogDef.comment.nonEmpty) {
      catSql.append(s"COMMENT '${catalogDef.comment.get}'")
    }
    (catSql.toString(), "CATALOG")
  }

  def externalLocationCreationQuery(extLocDef: ExternalLocationDefinition): (String, String) = {
    val externalLocSql = new StringBuilder(s"CREATE EXTERNAL LOCATION IF NOT EXISTS " +
      s"`${extLocDef.locationName}` " +
      s"URL '${extLocDef.locationUrl}' " +
      s"WITH (STORAGE CREDENTIAL `${extLocDef.storageCredentialName}`) ")
    if (extLocDef.comment.nonEmpty) {
      externalLocSql.append(s"COMMENT '${extLocDef.comment.get}'")
    }
    (externalLocSql.toString(), "EXTERNAL LOCATION")
  }

  def schemaCreationQuery(schemaDef: SchemaDefinition): (String, String) = {
    val schemaIdentifier = if (isUCEnabled) "SCHEMA" else "DATABASE"
    val locationIdentifier = if (isUCEnabled) "MANAGED LOCATION" else "LOCATION"

    val schemaCreateSQL = new StringBuilder(s"CREATE $schemaIdentifier IF NOT EXISTS " +
      s"${schemaDef.qualifiedSchemaName} ")
    if (schemaDef.locationUrl.nonEmpty) {
      schemaCreateSQL.append(s"$locationIdentifier '${schemaDef.locationUrl.get}' ")
    }
    if (schemaDef.comment.nonEmpty) {
      schemaCreateSQL.append(s"COMMENT '${schemaDef.comment.get}' ")
    }
    if (schemaDef.properties.nonEmpty) {
      val tableProperties = schemaDef.properties.map(_.productIterator.mkString("'", "'='", "'"))
        .mkString(",")
      schemaCreateSQL.append(s"WITH DBPROPERTIES($tableProperties)")
    }
    (schemaCreateSQL.toString(), schemaIdentifier)
  }

  def tableCreateQuery(tableDef: TableDefinition): (String, String) = {
    val tableCreateSQL =
      new StringBuilder(s"CREATE TABLE IF NOT EXISTS " +
        s"${tableDef.tableName} " +
        s"(${tableDef.schema.toDDL}) ")
    if (tableDef.partitionColumnNames.nonEmpty) {
      tableCreateSQL.append(s"""PARTITIONED BY (${tableDef.partitionColumnNames.mkString(",")}) """)
    }
    if (tableDef.locationUrl.nonEmpty) {
      tableCreateSQL.append(s"""LOCATION '${tableDef.locationUrl}' """)
    }
    if (tableDef.comment.nonEmpty) {
      tableCreateSQL.append(s"COMMENT '${tableDef.comment.get}'")
    }
    (tableCreateSQL.toString(), "TABLE")
  }

  def deleteDirectory(dirName: String): Boolean = {
    val fileSystem = FileSystem.get(new URI(dirName),
      SparkSession.active.sparkContext.hadoopConfiguration)
    fileSystem.delete(new Path(dirName), true)
  }

  def dropSchema(dbName: String): DataFrame = {
    val schemaDropSql = s"DROP SCHEMA IF EXISTS $dbName CASCADE"
    logInfo(s"Executing SQL : $schemaDropSql")
    SparkSession.active.sql(schemaDropSql)
  }

  def dropCatalog(catalogName: String): DataFrame = {
    val catalogDropSql = s"DROP CATALOG IF EXISTS $catalogName CASCADE"
    logInfo(s"Executing SQL : $catalogDropSql")
    SparkSession.active.sql(catalogDropSql)
  }

  def getDeltaWildCardPathUDF(): UserDefinedFunction = {
    udf((filePath: String, wildCardLevel: Int) => resolveWildCardPath(filePath, wildCardLevel))
  }

  def resolveWildCardPath(filePath: String, wildCardLevel: Int): String = {
    assert(wildCardLevel == -1 || wildCardLevel == 0 || wildCardLevel == 1,
      "WildCard Level should be -1, 0 or 1")
    val modifiedPath = if (wildCardLevel == 0) {
      (filePath.split("/").dropRight(1) :+ "*")
    } else if (wildCardLevel == 1) {
      (filePath.split("/").dropRight(2) :+ "*" :+ "*")
    } else {
      filePath.split("/")
    }
    modifiedPath.mkString("/") + "/_delta_log/*.json"
  }

  def consolidateWildCardPaths(wildCardPaths: Seq[(String, String)]): Seq[(String, String)] = {
    wildCardPaths.foldLeft(ListBuffer.empty[(String, String)]) { (l, a) =>
      if (l.nonEmpty) {
        val split_a = a._1.split("\\*")(0)
        for (b <- l.toList) {
          val split_b = b._1.split("\\*")(0)
          if (split_b contains split_a) {
            l -= b
            if (!l.contains(a)) {
              l += a
            }
          } else {
            if (!(split_a contains split_b) && !l.contains(a)) {
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
    val subDirectories = listSubDirectories(sourceConfig.path, conf)
    subDirectories.map(d => SourceConfig(d, skipProcessing))
  }

  def listSubDirectories(path: String, conf: SerializableConfiguration): Array[String] = {
    val fs = new Path(path).getFileSystem(conf.value)
    fs.listStatus(new Path(path)).filter(_.isDirectory).map(_.getPath.toString)
  }

  def recursiveListDeltaTablePaths(sourceConfig: SourceConfig, conf: SerializableConfiguration):
  Set[SourceConfig] = {
    val skipProcessing = sourceConfig.skipProcessing
    recursiveListDeltaTablePaths(sourceConfig.path, conf)
      .map(d => SourceConfig(d, skipProcessing))
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

  // Legacy Support Methods
  def dropDatabase(dbName: String): DataFrame = {
    val dbDropSql = s"DROP DATABASE IF EXISTS $dbName CASCADE"
    logInfo(s"Executing SQL : $dbDropSql")
    SparkSession.active.sql(dbDropSql)
  }
}

object UtilityOperations extends UtilityOperations
