package com.quasardb.spark.rdd

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import com.quasardb.spark.Configuration

object Util {

  def withRetries[T](f: () => T) {
    println("retrying...")
  }

}
