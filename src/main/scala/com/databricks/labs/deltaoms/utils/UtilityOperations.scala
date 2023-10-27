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
    if (tableDef.properties.nonEmpty) {
      val tableProperties = tableDef.properties.map(_.productIterator.mkString("'", "'='", "'"))
        .mkString(",")
      tableCreateSQL.append(s"TBLPROPERTIES($tableProperties) ")
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

  // Legacy Support Methods
  def dropDatabase(dbName: String): DataFrame = {
    val dbDropSql = s"DROP DATABASE IF EXISTS $dbName CASCADE"
    logInfo(s"Executing SQL : $dbDropSql")
    SparkSession.active.sql(dbDropSql)
  }
}

object UtilityOperations extends UtilityOperations
