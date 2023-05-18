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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}

class OMSStreamingQueryListener extends StreamingQueryListener with Logging {

  override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
    logInfo(s"Query=${queryStarted.name}:QueryId=${queryStarted.id}:STARTED:" +
      s"RunId=${queryStarted.runId}:Timestamp=${queryStarted.timestamp}")
  }

  override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
    logInfo(s"QueryId=${queryTerminated.id}:RunId=${queryTerminated.runId}:TERMINATED:" +
      s"Exception: ${queryTerminated.exception.toString}"
    )
  }

  override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
    if (queryProgress.progress.numInputRows > 0) {
      logInfo(s"Query Progress: ${queryProgress.progress.prettyJson}")
    }
  }
}
