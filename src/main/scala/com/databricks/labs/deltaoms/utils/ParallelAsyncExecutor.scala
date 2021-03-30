package com.databricks.labs.deltaoms.utils

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.duration.Duration._
import scala.concurrent.{Future, _}

object ParallelAsyncExecutor {

  def executeAsync[T](f: => T): Future[T] = {
    Future(f)
  }

  def awaitSliding[T](it: Iterator[Future[T]], batchSize: Int = 3, timeout: Duration = Inf): Iterator[T] = {
    val (firstElements, lastElement) = it.grouped(batchSize)
      .sliding(2)
      .span(_ => it.hasNext)

    (firstElements.map(_.head) ++ lastElement.flatten).flatten.map(Await.result(_, timeout))
  }
}
