package com.databricks.labs.deltaoms.common

import com.databricks.labs.deltaoms.configuration.SparkSettings
import org.apache.spark.internal.Logging


trait OMSRunner extends Serializable with SparkSettings with OMSOperations with Logging {
}
