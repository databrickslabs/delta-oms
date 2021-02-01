package com.databricks.labs.deltaods.utils

import org.apache.spark.internal.Logging

trait ODSUtils extends Serializable with Logging {

  def ODSConfigToTableDefinition()

}
