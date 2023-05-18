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

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.databricks.labs.deltaoms.common.OMSSparkConf.consolidateOMSConfigFromSparkConf
import com.databricks.labs.deltaoms.configuration.{OMSConfig, SparkSettings}
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils

import org.apache.spark.internal.Logging

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

  scala.util.control.Exception.ignoring(classOf[Throwable]) {
    setTrackingHeader()
  }

  def getValidatedOMSConfig(config: OMSConfig): OMSConfig

  def consolidateOMSConfig(): OMSConfig = {
    val sparkOMSConfig = consolidateOMSConfigFromSparkConf(omsConfig)
    getValidatedOMSConfig(sparkOMSConfig)
  }
}

trait BatchOMSRunner extends OMSRunner {
  def getValidatedOMSConfig(config: OMSConfig): OMSConfig = {
    validateOMSConfig(config)
    config
  }
}

trait StreamOMSRunner extends OMSRunner {
  def getValidatedOMSConfig(config: OMSConfig): OMSConfig = {
    validateOMSConfig(config, isBatch = false)
    config
  }
}

