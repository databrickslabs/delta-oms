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

  logInfo(s"Loading configuration from : ${environmentType}")
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

  scala.util.control.Exception.ignoring(classOf[Throwable]) { setTrackingHeader() }

  def validateOMSConfig(config: OMSConfig): OMSConfig

  def fetchConsolidatedOMSConfig(args: Array[String]) : OMSConfig = {
    validateOMSConfig(OMSSparkConf.consolidateOMSConfigFromSparkConf(omsConfig))
  }
}

private object ValidationUtils {
  def validateOMSConfig(omsConfig: OMSConfig, isBatch: Boolean = true): Unit = {
    assert(omsConfig.locationUrl.isDefined,
      "Mandatory configuration OMS Location URL missing. " +
        "Provide through Command line argument --locationUrl")
    assert(omsConfig.locationName.isDefined,
      "Mandatory configuration OMS Location Name missing. " +
        "Provide through Command line argument --locationName")
    assert(omsConfig.storageCredentialName.isDefined,
      "Mandatory configuration OMS Storage Credential Name missing. " +
        "Provide through Command line argument --locationName")
    assert(omsConfig.catalogName.isDefined,
      "Mandatory configuration OMS Catalog Name missing. " +
        "Provide through Command line argument --catalogName")
    assert(omsConfig.schemaName.isDefined,
      "Mandatory configuration OMS Schema Name missing. " +
        "Provide through Command line argument --schemaName")
    if(!isBatch) {
      assert(omsConfig.checkpointBase.isDefined,
        "Mandatory configuration OMS Checkpoint Base Location missing. " +
          "Provide through Command line argument --checkpointBase")
      assert(omsConfig.checkpointSuffix.isDefined,
        "Mandatory configuration OMS Checkpoint Suffix missing. " +
          "Provide through Command line argument --checkpointSuffix")
    }
  }
}

trait BatchOMSRunner extends OMSRunner {
  def validateOMSConfig(config: OMSConfig): OMSConfig = {
    ValidationUtils.validateOMSConfig(omsConfig)
    config
  }
}

trait StreamOMSRunner extends OMSRunner {
  def validateOMSConfig(config: OMSConfig): OMSConfig = {
    ValidationUtils.validateOMSConfig(omsConfig, isBatch = false)
    config
  }
}

