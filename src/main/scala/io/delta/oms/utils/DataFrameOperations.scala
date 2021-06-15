/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.oms.utils

import org.apache.spark.sql.Dataset

object DataFrameOperations {

  implicit class DataFrameOps[T](val logData: Dataset[T]) extends AnyVal {
    def writeDataToDeltaTable(path: String,
      mode: String = "append",
      partitionColumnNames: Seq[String] = Seq.empty[String]): Unit = {
      val dw = logData.write.mode(mode).format("delta")
      if (partitionColumnNames.nonEmpty) {
        dw.partitionBy(partitionColumnNames: _*).save(path)
      } else {
        dw.save(path)
      }
    }
  }

}
