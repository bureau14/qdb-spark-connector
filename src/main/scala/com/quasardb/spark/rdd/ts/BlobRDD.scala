package net.quasardb.spark.rdd.ts

import java.nio.ByteBuffer
import java.sql.Timestamp
import scala.collection.JavaConversions._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, Row, DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark._

import net.quasardb.qdb._
import net.quasardb.spark.partitioner._
import net.quasardb.spark.rdd.Util

class BlobRDD(
  sc: SparkContext,
  val uri: String,
  val table: String,
  val column: String,
  val ranges: QdbTimeRangeCollection)(implicit securityOptions : Option[QdbSession.SecurityOptions])
    extends RDD[(Timestamp, Array[Byte])](sc, Nil) {

  override protected def getPartitions = QdbPartitioner.computePartitions(uri)

  override def compute(
    split: Partition,
    context: TaskContext): Iterator[(Timestamp, Array[Byte])] = {
    val partition: QdbPartition = split.asInstanceOf[QdbPartition]

    val series: QdbTimeSeries = Util.createCluster(partition.uri).timeSeries(table)

    // TODO: limit query to only the Partition
    series.getBlobs(column, ranges).toList.map(BlobRDD.fromJava).iterator
  }

  def toDataFrame(sqlContext: SQLContext): DataFrame = {
    val struct =
      StructType(
        StructField("timestamp", TimestampType, false) ::
        StructField("value", BinaryType, false) :: Nil)

    sqlContext.createDataFrame(map(BlobRDD.toRow), struct(Set("timestamp", "value")))
  }
}

object BlobRDD {
  def fromJava(row:QdbBlobColumnValue):(Timestamp, Array[Byte]) = {
    val buf : ByteBuffer = row.getValue
    var arr : Array[Byte] = new Array[Byte](buf.remaining)
    buf.get(arr)
    buf.rewind

    (row.getTimestamp.asTimestamp, arr)
  }

  def toJava(row:(Timestamp, Array[Byte])):QdbBlobColumnValue = {
    val buf : ByteBuffer = ByteBuffer.allocateDirect(row._2.length)
    buf.put(row._2)
    buf.rewind()

    new QdbBlobColumnValue(row._1, buf)
  }

  def fromRow(row:Row):(Timestamp, Array[Byte]) = {
    (row.getTimestamp(0), row.getAs[Array[Byte]](1))
  }

  def toRow(row:(Timestamp, Array[Byte])): Row = {
    Row(row._1, row._2)
  }
}
