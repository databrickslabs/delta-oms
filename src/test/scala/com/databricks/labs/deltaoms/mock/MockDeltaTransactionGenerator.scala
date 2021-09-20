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

package com.databricks.labs.deltaoms.mock

import java.nio.file.{Paths}

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

trait MockDeltaTransactionGenerator {

  protected def spark: SparkSession

  case class MockDBInfo(dbName: String, basePath: String)
  case class MockTableInfo(db: MockDBInfo, tableName: String,
    partitionColumns: Option[Seq[String]] = None)

  def cleanupDatabase(db: MockDBInfo): Unit = {
    val dbLocation = s"${db.basePath}/${db.dbName}"
    // Drop Database
    spark.sql(s"DROP DATABASE IF EXISTS ${db.dbName} CASCADE")
    // Cleanup location
    org.apache.spark.network.util.JavaUtils.deleteRecursively(Paths.get(dbLocation).toFile)
  }

  def createDatabase(db: MockDBInfo): DataFrame = {
    cleanupDatabase(db)
    val dbLocation = s"${db.basePath}/${db.dbName}"
    // Create Database
    spark.sql(s"CREATE DATABASE IF NOT EXISTS ${db.dbName} LOCATION '$dbLocation'")
  }

  // Generate Mock Transactions
  def executeTestDeltaOperations(tableDefn: MockTableInfo): DataFrame = {
    val dbName = tableDefn.db.dbName
    val basePath = tableDefn.db.basePath
    val tableName = tableDefn.tableName
    val fullTableName = s"$dbName.$tableName"
    val dbLocation = s"$basePath/$dbName"
    val tableLocation = s"$dbLocation/$tableName/"

    // Create and populate table
    if(tableDefn.partitionColumns.isDefined) {
      val tablePartitions = tableDefn.partitionColumns.get
      spark.range(1, 1000, 1, 100)
        .withColumn("uid", col("id")%10)
        .write
        .format("delta")
        .mode("overwrite").partitionBy(tablePartitions: _*)
        .save(s"$tableLocation")
    } else {
      spark.range(1, 1000, 1, 100)
        .write
        .format("delta")
        .mode("overwrite")
        .save(s"$tableLocation")
    }
    val createTableSQL = s"CREATE TABLE IF NOT EXISTS $fullTableName " +
      s"USING DELTA LOCATION '$tableLocation'"
    spark.sql(createTableSQL)
    // spark.sql(s"SHOW TABLES IN $dbName").show()
    // spark.sql(s"DESCRIBE EXTENDED $fullTableName").show(false)
    // Insert more data
    if(tableDefn.partitionColumns.isDefined) {
      spark.sql(s"INSERT into $fullTableName " +
        s"VALUES (1000000,100000),(1000001, 100000),(1000002, 100000)")
    } else {
      spark.sql(s"INSERT into $fullTableName VALUES (1000000),(1000001),(1000002)")
    }
    // Update a row
    spark.sql(s"UPDATE $fullTableName SET id=1999 WHERE id = 999")
    // Delete some data
    spark.sql(s"DELETE FROM $fullTableName WHERE id%200 = 0")

    // Upsert new data
    if(tableDefn.partitionColumns.isDefined) {
      spark.range(1, 1200, 100)
        .withColumn("uid", col("id")%10)
        .createOrReplaceTempView("TEMP_UPSERTS")
      spark.sql(
        s"""MERGE INTO $fullTableName a
       USING TEMP_UPSERTS b
       ON a.id=b.id AND a.uid = b.uid
       WHEN MATCHED THEN UPDATE SET *
       WHEN NOT MATCHED THEN INSERT *
    """)
    } else {
      spark.range(1, 1200, 100)
        .createOrReplaceTempView("TEMP_UPSERTS")
      spark.sql(
        s"""MERGE INTO $fullTableName a
       USING TEMP_UPSERTS b
       ON a.id=b.id
       WHEN MATCHED THEN UPDATE SET *
       WHEN NOT MATCHED THEN INSERT *
    """)
    }
  }

  def createMockDatabaseAndTables(testTables : Seq[MockTableInfo]): Unit = {
    val testDatabases = testTables.map(_.db).distinct
    testDatabases.foreach(createDatabase)
    testTables.foreach(executeTestDeltaOperations)
  }

  def cleanupDatabases(testTables : Seq[MockTableInfo]): Unit = {
    val testDatabases = testTables.map(_.db).distinct
    testDatabases.foreach(cleanupDatabase)
  }

}
