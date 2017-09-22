package com.quasardb.spark.rdd

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

class QdbTimeSeriesBlobRDD(
  sc: SparkContext,
  val uri: String,
  val table: String,
  val column: String,
  val ranges: QdbTimeRangeCollection)
    extends RDD[QdbBlobColumnValue](sc, Nil) {

  override protected def getPartitions = QdbPartitioner.computePartitions(uri)

  override def compute(
    split: Partition,
    context: TaskContext): Iterator[QdbBlobColumnValue] = {
    val partition: QdbPartition = split.asInstanceOf[QdbPartition]

    val series: QdbTimeSeries = new QdbCluster(partition.uri).timeSeries(table)

    // TODO: limit query to only the Partition
    series.getBlobs(column, ranges).toList.iterator
  }

  def toDataFrame(sqlContext: SQLContext): DataFrame = {
    val struct =
      StructType(
        StructField("timestamp", TimestampType, false) ::
        StructField("value", BinaryType, false) :: Nil)

    sqlContext.createDataFrame(map(QdbTimeSeriesBlobRDD.toRow), struct(Set("timestamp", "value")))
  }
}

object QdbTimeSeriesBlobRDD {
  def toRow(row:QdbBlobColumnValue): Row = {
    val buf : ByteBuffer = row.getValue
    var arr : Array[Byte] = new Array[Byte](buf.remaining)
    buf.get(arr)

    Row(Timestamp.valueOf(row.getTimestamp.getValue), arr)
  }
}
