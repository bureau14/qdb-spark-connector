package net.quasardb.spark.rdd

import java.sql.Timestamp

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import odelay.Timer

import net.quasardb.qdb._

import net.quasardb.spark.connection.QdbConnection
import net.quasardb.spark.rdd.ts.{DoubleRDD, BlobRDD}

import retry._
import retry.Success

object Util {

  def createCluster(uri: String)
    (implicit securityOptions : Option[QdbSession.SecurityOptions]) : QdbCluster = securityOptions match {
    case Some(securityOptions) => new QdbCluster(uri, securityOptions)
    case None => new QdbCluster(uri)
  }

  def appendRows(
    uri: String,
    table: String,
    values: Iterator[QdbTimeSeriesRow])(implicit securityOptions : Option[QdbSession.SecurityOptions]): Unit = {
    implicit val success = Success[Boolean](_ == true)
    implicit val timer = odelay.Timer.default

    val (begin, copy) = values.duplicate

    try {
      val localTable : QdbTimeSeriesTable =
        new QdbConnection().cluster(uri).timeSeries(table).table()

      copy.foreach { localTable.append(_) }
      localTable.flush
    } catch {

      // Thrown in case of race condition
      case e: QdbOperationException =>
        appendRows(uri, table, begin)
    }
  }

  def insertDoubles(
    uri: String,
    table: String,
    column: String,
    values: Iterator[(Timestamp, Double)])(implicit securityOptions : Option[QdbSession.SecurityOptions]): Unit = {

    var collection = new QdbDoubleColumnCollection(column)
    collection.addAll(values.map(DoubleRDD.toJava).toList)

    implicit val success = Success[Boolean](_ == true)
    implicit val timer = odelay.Timer.default

    val future = retry.Backoff(8, 50.millis)(timer) { () =>

      try {
        createCluster(uri)
          .timeSeries(table)
          .insertDoubles(collection)

        Future.successful(true)
      } catch {

        // Thrown in case of race condition
        case e: QdbOperationException =>
          Future.failed(e)
      }
    }

    Await.result(future, 30.second)
  }

  def insertBlobs(
    uri: String,
    table: String,
    column: String,
    values: Iterator[(Timestamp, Array[Byte])])(implicit securityOption : Option[QdbSession.SecurityOptions]): Unit = {

    var collection = new QdbBlobColumnCollection(column)
    collection.addAll(values.map(BlobRDD.toJava).toList)

    implicit val success = Success[Boolean](_ == true)
    implicit val timer = odelay.Timer.default

    val future = retry.Backoff(8, 50.millis)(timer) { () =>

      try {
        createCluster(uri)
          .timeSeries(table)
          .insertBlobs(collection)

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
