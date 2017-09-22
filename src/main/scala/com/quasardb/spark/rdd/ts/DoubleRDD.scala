package com.quasardb.spark.rdd.ts

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

import com.quasardb.spark.partitioner._

class DoubleRDD(
  sc: SparkContext,
  val uri: String,
  val table: String,
  val column: String,
  val ranges: QdbTimeRangeCollection)
    extends RDD[(Timestamp, Double)](sc, Nil) {

  override protected def getPartitions = QdbPartitioner.computePartitions(uri)

  override def compute(
    split: Partition,
    context: TaskContext): Iterator[(Timestamp, Double)] = {
    val partition: QdbPartition = split.asInstanceOf[QdbPartition]

    val series: QdbTimeSeries = new QdbCluster(partition.uri).timeSeries(table)

    // TODO: limit query to only the Partition
    series.getDoubles(column, ranges).toList.map(DoubleRDD.fromJava).iterator
  }

  def toDataFrame(sqlContext: SQLContext): DataFrame = {
    val struct =
      StructType(
        StructField("timestamp", TimestampType, false) ::
        StructField("value", DoubleType, false) :: Nil)


    sqlContext.createDataFrame(map(DoubleRDD.toRow), struct(Set("timestamp", "value")))
  }

  def saveToQdbDoubleColumn(
    uri: String,
    table: String,
    column: String) : Unit = {

    return null;
  }
}

object DoubleRDD {
  def fromJava(row:QdbDoubleColumnValue):(Timestamp, Double) = {
    (Timestamp.valueOf(row.getTimestamp.getValue), row.getValue)
  }

  def toJava(row:(Timestamp, Double)):QdbDoubleColumnValue = {
    new QdbDoubleColumnValue(row._1, row._2)
  }

  def toRow(row:(Timestamp, Double)): Row = {
    Row(row._1, row._2)
  }
}
