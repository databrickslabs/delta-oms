package com.databricks.labs.deltaods.common

sealed trait Environment

case object InBuilt extends Environment

case object Local extends Environment

case object Remote extends Environment

object EnvironmentResolver {
  def fetchEnvironment(envStr: String): Environment = {
    if (envStr.contains("inbuilt")) InBuilt
    else if (envStr.startsWith("file:/")) Local
    else Remote
  }
}