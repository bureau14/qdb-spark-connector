package com.quasardb.spark.rdd

import java.sql.Timestamp

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import odelay.Timer

import net.quasardb.qdb._

import com.quasardb.spark.rdd.ts.DoubleRDD

import retry._
import retry.Success

object Util {

  def insertDoubles(
    uri: String,
    table: String,
    column: String,
    values: Iterator[(Timestamp, Double)]): Unit = {

    var collection = new QdbDoubleColumnCollection(column)
    collection.addAll(values.map(DoubleRDD.toJava).toList)

    implicit val success = Success[Boolean](_ == true)
    implicit val timer = odelay.Timer.default

    val future = retry.Backoff(8, 50.millis)(timer) { () =>

      try {
        new QdbCluster(uri).timeSeries(table).insertDoubles(collection)
        Future.successful(true)
      } catch {

        // Thrown in case of race condition
        case e: QdbOperationException =>
          Future.failed(e)
      }
    }

    Await.result(future, 30.second)
  }
}
