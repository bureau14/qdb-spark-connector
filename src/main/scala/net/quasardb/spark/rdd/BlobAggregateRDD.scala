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

import net.quasardb.spark.rdd.{AggregateQuery, Util}
import net.quasardb.spark.partitioner._

case class BlobAggregation(
  count: Long,
  result: Array[Byte]
) extends Serializable

class BlobAggregateRDD(
  sc: SparkContext,
  val uri: String,
  val table: String,
  val column: String,
  val input: Seq[AggregateQuery])(implicit securityOptions : Option[QdbSession.SecurityOptions])
    extends RDD[BlobAggregation](sc, Nil) {

  override protected def getPartitions = QdbPartitioner.computePartitions(uri)

  override def compute(
    split: Partition,
    context: TaskContext): Iterator[BlobAggregation] = {

    val aggregate = new QdbBlobAggregationCollection()

    input.foreach {
      _ match {
        case AggregateQuery(begin, end, operation) => aggregate.add(
          new QdbBlobAggregation(
            operation,
            new QdbTimeRange(
              new QdbTimespec(begin),
              new QdbTimespec(end))))
      }
    }

    val partition: QdbPartition = split.asInstanceOf[QdbPartition]
    val series: QdbTimeSeries = Util.createCluster(partition.uri).timeSeries(table)

    // TODO: limit query to only the Partition
    series.blobAggregate(column, aggregate).toList.map(BlobAggregateRDD.fromJava).iterator
  }

  def toDataFrame(sqlContext: SQLContext): DataFrame = {
    val struct =
      StructType(
        StructField("count", LongType, true) ::
        StructField("result", BinaryType, true) :: Nil)

    sqlContext.createDataFrame(map(BlobAggregateRDD.toRow), struct(Set("count", "result")))
  }
}

object BlobAggregateRDD {
  def fromJava(row:QdbBlobAggregation):BlobAggregation = {
    val buf : ByteBuffer = row.getResult.getValue
    var arr : Array[Byte] = new Array[Byte](buf.remaining)
    buf.get(arr)
    buf.rewind

    BlobAggregation(
      row.getCount,
      arr)
  }

  def toRow(row:BlobAggregation): Row = {
    Row(
      row.count,
      row.result)
  }
}
