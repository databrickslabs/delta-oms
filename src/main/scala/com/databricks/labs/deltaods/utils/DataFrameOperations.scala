package com.databricks.labs.deltaods.utils

import org.apache.spark.sql.Dataset

object DataFrameOperations {
  implicit class DataFrameOps[T](val logData: Dataset[T]) extends AnyVal {
    def writeDataToDeltaTable(path: String,
                              mode: String = "append",
                              partitionColumnNames: Seq[String] = Seq.empty[String]) = {
      val dw = logData.write.mode(mode).format("delta")
      if(partitionColumnNames.nonEmpty){
        dw.partitionBy(partitionColumnNames: _*).save(path)
      } else {
        dw.save(path)
      }
    }
  }
}
