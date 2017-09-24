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

case class DoubleAggregation(
  begin: Timestamp,
  end: Timestamp,
  count: Long,
  result: Double
) extends Serializable

class DoubleAggregateRDD(
  sc: SparkContext,
  val uri: String,
  val table: String,
  val column: String,
  val input: Seq[(Timestamp, Timestamp, QdbAggregation.Type)])
    extends RDD[DoubleAggregation](sc, Nil) {

  override protected def getPartitions = QdbPartitioner.computePartitions(uri)

  override def compute(
    split: Partition,
    context: TaskContext): Iterator[DoubleAggregation] = {

    val aggregate = new QdbDoubleAggregationCollection()

    input.foreach { agg =>
      aggregate.add(
        new QdbDoubleAggregation(
          agg._3,
          new QdbTimeRange(
            new QdbTimespec(agg._1),
            new QdbTimespec(agg._2)))) }

    val partition: QdbPartition = split.asInstanceOf[QdbPartition]
    val series: QdbTimeSeries = new QdbCluster(partition.uri).timeSeries(table)

    // TODO: limit query to only the Partition
    series.doubleAggregate(column, aggregate).toList.map(DoubleAggregateRDD.fromJava).iterator
  }

  def toDataFrame(sqlContext: SQLContext): DataFrame = {
    val struct =
      StructType(
        StructField("begin", TimestampType, false) ::
        StructField("end", TimestampType, false) ::
        StructField("count", LongType, true) ::
        StructField("result", DoubleType, true) :: Nil)

    sqlContext.createDataFrame(map(DoubleAggregateRDD.toRow), struct(Set("begin", "end", "count", "result")))
  }
}

object DoubleAggregateRDD {
  def fromJava(row:QdbDoubleAggregation):DoubleAggregation = {
    DoubleAggregation(
      Timestamp.valueOf(row.getRange.getBegin.getValue),
      Timestamp.valueOf(row.getRange.getEnd.getValue),
      row.getCount,
      row.getResult.getValue)
  }

  def toRow(row:DoubleAggregation): Row = {
    Row(
      row.begin,
      row.end,
      row.count,
      row.result)
  }
}
