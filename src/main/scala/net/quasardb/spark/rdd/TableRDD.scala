package net.quasardb.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, Row, DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark._

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8
import java.sql.Timestamp

import net.quasardb.qdb._

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

import net.quasardb.spark.partitioner._
import net.quasardb.spark.rdd.Util

class TableRDD(
  sc: SparkContext,
  val uri: String,
  val table: String,
  val ranges: QdbTimeRangeCollection)(implicit securityOptions : Option[QdbSession.SecurityOptions])
    extends RDD[QdbTimeSeriesRow](sc, Nil) {

  override protected def getPartitions = QdbPartitioner.computePartitions(uri)

  override def compute(
    split: Partition,
    context: TaskContext): Iterator[QdbTimeSeriesRow] = {
    // Not implemented yet
    val emptyList : List[QdbTimeSeriesRow] = List()
    emptyList.iterator
  }
}

object TableRDD {
  def fromJava(row:QdbDoubleColumnValue):(Timestamp, Double) = {
    (row.getTimestamp.asTimestamp, row.getValue)
  }

  def toJava(row:(Timestamp, Double)):QdbDoubleColumnValue = {
    new QdbDoubleColumnValue(row._1, row._2)
  }

  def fromRow(row:Row):(Timestamp, Double) = {
    (row.getTimestamp(0), row.getDouble(1))
  }

  def toRow(row:(Timestamp, Double)): Row = {
    Row(row._1, row._2)
  }
}
