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

package com.databricks.labs.deltaoms.utils

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.duration.Duration._
import scala.concurrent.{Future, _}

object ParallelAsyncExecutor {

  def executeAsync[T](f: => T): Future[T] = {
    Future(f)
  }

  def awaitSliding[T](it: Iterator[Future[T]], batchSize: Int = 3, timeout: Duration = Inf)
  : Iterator[T] = {
    val (firstElements, lastElement) = it.grouped(batchSize)
      .sliding(2)
      .span(_ => it.hasNext)
    // scalastyle:off awaitresult
    (firstElements.map(_.head) ++ lastElement.flatten).flatten.map(Await.result(_, timeout))
    // scalastyle:on awaitresult
  }
}
