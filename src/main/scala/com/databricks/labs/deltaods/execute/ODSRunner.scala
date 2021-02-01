package com.databricks.labs.deltaods.execute

import com.databricks.labs.deltaods.common.{ODSOperations, SparkSettings}
import org.apache.spark.internal.Logging


trait ODSRunner extends Serializable with SparkSettings with ODSOperations with Logging {
}
