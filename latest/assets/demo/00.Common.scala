// Databricks notebook source
case class DBInfo(dbName: String, basePath: String = "dbfs:/user/hive/warehouse")
case class TableInfo(db: DBInfo, tableName: String, partitionColumns: Option[Seq[String]] = None)

// COMMAND ----------

def cleanupDatabase(db: DBInfo) = {
  val dbLocation = s"${db.basePath}/${db.dbName}"
  //Drop Database
  spark.sql(s"DROP DATABASE IF EXISTS ${db.dbName} CASCADE")
  //Cleanup location
  dbutils.fs.rm(dbLocation, true)
}

def createDatabase(db: DBInfo) = {
  cleanupDatabase(db)
  val dbLocation = s"${db.basePath}/${db.dbName}"
  // Create Database
  spark.sql(s"CREATE DATABASE IF NOT EXISTS ${db.dbName} LOCATION '$dbLocation'")
}

//Generate Mock Transactions
def executeTestDeltaOperations(tableDefn: TableInfo) = {
  
  val dbName = tableDefn.db.dbName
  val basePath = tableDefn.db.basePath
  val tableName = tableDefn.tableName
  val fullTableName = s"$dbName.$tableName"
  val dbLocation = s"$basePath/$dbName"
  val tableLocation = s"$dbLocation/$tableName/"
  
  //Create and populate table
  if(tableDefn.partitionColumns.isDefined){
    val tablePartitions = tableDefn.partitionColumns.get
    spark.range(1,1000,1, 100).withColumn("uid",$"id"%10).write.format("delta").mode("overwrite").partitionBy(tablePartitions: _*).save(s"$tableLocation")
  } else {
    spark.range(1,1000,1, 100).write.format("delta").mode("overwrite").save(s"$tableLocation")
  }
  spark.sql(s"CREATE TABLE IF NOT EXISTS ${fullTableName} USING DELTA LOCATION '$tableLocation'")
  //Insert more data
  if(tableDefn.partitionColumns.isDefined) {
    spark.sql(s"INSERT into ${fullTableName} VALUES (1000000,100000),(1000001, 100000),(1000002, 100000)")
  } else {
    spark.sql(s"INSERT into ${fullTableName} VALUES (1000000),(1000001),(1000002)")
  }
  //Update a row
  spark.sql(s"UPDATE ${fullTableName} SET id=1999 WHERE id = 999")
  // Delete some data
  spark.sql(s"DELETE FROM ${fullTableName} WHERE id%200 = 0")
  //Optimize the table
  if(tableDefn.partitionColumns.isDefined) {
    spark.sql(s"OPTIMIZE ${fullTableName}")
  } else {
    spark.sql(s"OPTIMIZE ${fullTableName} ZORDER BY id")
  }
  //Upsert new data
  if(tableDefn.partitionColumns.isDefined) {
    spark.range(1,1200,100).withColumn("uid",$"id"%10).createOrReplaceTempView("TEMP_UPSERTS")
    spark.sql(
    s"""MERGE INTO ${fullTableName} a
       USING TEMP_UPSERTS b
       ON a.id=b.id AND a.uid = b.uid
       WHEN MATCHED THEN UPDATE SET *
       WHEN NOT MATCHED THEN INSERT *
    """)
  } else {
    spark.range(1,1200,100).createOrReplaceTempView("TEMP_UPSERTS")
    spark.sql(
    s"""MERGE INTO ${fullTableName} a
       USING TEMP_UPSERTS b
       ON a.id=b.id
       WHEN MATCHED THEN UPDATE SET *
       WHEN NOT MATCHED THEN INSERT *
    """)
  }
}

def createMockDatabaseAndTables(testTables : Seq[TableInfo]) = {
  val testDatabases = testTables.map(_.db).distinct
  testDatabases.foreach(createDatabase)
  testTables.foreach(executeTestDeltaOperations)
}

def cleanupDatabases(testTables : Seq[TableInfo]) = {
  val testDatabases = testTables.map(_.db).distinct
  testDatabases.foreach(cleanupDatabase)
}
