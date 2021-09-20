/*
 * Copyright (2021) Databricks, Inc.
 *
 * Delta Operational Metrics Store(DeltaOMS)
 *
 * Copyright 2021 Databricks, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.databricks.labs.deltaoms.common

import com.databricks.labs.deltaoms.configuration.{OMSConfig, SparkSettings}

import org.apache.spark.internal.Logging
import org.apache.http.impl.client.HttpClients
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.util.EntityUtils
import org.apache.http.client.config.RequestConfig
import com.databricks.dbutils_v1.DBUtilsHolder.dbutils

trait OMSRunner extends Serializable
  with SparkSettings
  with OMSInitializer
  with OMSOperations
  with Logging {

  logInfo(s"Loading configuration from : ${environmentConfigFile}")
  logInfo(s"Environment set to : ${environment}")
  logInfo(s"OMS Config from configuration file : ${omsConfig}")

  // Add Usage tracking calls

  def setTrackingHeader(): Unit = {
    val ntbCtx = dbutils.notebook.getContext()
    val apiUrl = ntbCtx.apiUrl.get
    val apiToken = ntbCtx.apiToken.get
    val clusterId = ntbCtx.clusterId.get

    val trackingHeaders = Seq[(String, String)](
      ("Content-Type", "application/json"),
      ("Charset", "UTF-8"),
      ("User-Agent", s"databricks-labs-deltaoms/${OMS_VERSION}"),
      ("Authorization", s"Bearer ${apiToken}"))

    val timeout = 30 * 1000

    val httpClient = HttpClients.createDefault()
    val getClusterByIdGet = new HttpGet(s"${apiUrl}/api/2.0/clusters/get?cluster_id=${clusterId}")
    val requestConfig: RequestConfig = RequestConfig.custom
      .setConnectionRequestTimeout(timeout)
      .setConnectTimeout(timeout)
      .setSocketTimeout(timeout)
      .build

    trackingHeaders.foreach(hdr => getClusterByIdGet.addHeader(hdr._1, hdr._2))
    getClusterByIdGet.setConfig(requestConfig)
    val response = httpClient.execute(getClusterByIdGet)
    logInfo(EntityUtils.toString(response.getEntity, "UTF-8"))
  }
  setTrackingHeader()

  def consolidateAndValidateOMSConfig(args: Array[String], config: OMSConfig): OMSConfig

  def fetchConsolidatedOMSConfig(args: Array[String]) : OMSConfig = {
    val sparkOMSConfig = consolidateOMSConfigFromSparkConf(omsConfig)
    consolidateAndValidateOMSConfig(args, sparkOMSConfig)
  }

  def consolidateOMSConfigFromSparkConf(config: OMSConfig): OMSConfig = {
    OMSSparkConf.consolidateOMSConfigFromSparkConf(config)
  }
}

trait BatchOMSRunner extends OMSRunner {
  def consolidateAndValidateOMSConfig(args: Array[String], config: OMSConfig): OMSConfig = {
    OMSCommandLineParser.consolidateAndValidateOMSConfig(args, omsConfig)
  }
}

trait StreamOMSRunner extends OMSRunner{
  def consolidateAndValidateOMSConfig(args: Array[String], config: OMSConfig): OMSConfig = {
    OMSCommandLineParser.consolidateAndValidateOMSConfig(args, omsConfig, isBatch = false)
  }
}

