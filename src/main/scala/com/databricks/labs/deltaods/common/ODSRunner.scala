package com.databricks.labs.deltaods.common

import com.databricks.labs.deltaods.configuration.SparkSettings
import org.apache.spark.internal.Logging


trait ODSRunner extends Serializable with SparkSettings with ODSOperations with Logging {
}
