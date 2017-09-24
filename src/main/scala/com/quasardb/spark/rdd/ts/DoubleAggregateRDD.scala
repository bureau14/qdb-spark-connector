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

import com.quasardb.spark.rdd.AggregateQuery
import com.quasardb.spark.partitioner._

case class DoubleAggregation(
  count: Long,
  result: Double
) extends Serializable

class DoubleAggregateRDD(
  sc: SparkContext,
  val uri: String,
  val table: String,
  val column: String,
  val input: Seq[AggregateQuery])
    extends RDD[DoubleAggregation](sc, Nil) {

  override protected def getPartitions = QdbPartitioner.computePartitions(uri)

  override def compute(
    split: Partition,
    context: TaskContext): Iterator[DoubleAggregation] = {

    val aggregate = new QdbDoubleAggregationCollection()

    input.foreach {
      _ match {
        case AggregateQuery(begin, end, operation) => aggregate.add(
          new QdbDoubleAggregation(
            operation,
            new QdbTimeRange(
              new QdbTimespec(begin),
              new QdbTimespec(end))))
      }
    }

    val partition: QdbPartition = split.asInstanceOf[QdbPartition]
    val series: QdbTimeSeries = new QdbCluster(partition.uri).timeSeries(table)

    // TODO: limit query to only the Partition
    series.doubleAggregate(column, aggregate).toList.map(DoubleAggregateRDD.fromJava).iterator
  }

  def toDataFrame(sqlContext: SQLContext): DataFrame = {
    val struct =
      StructType(
        StructField("count", LongType, true) ::
        StructField("result", DoubleType, true) :: Nil)

    sqlContext.createDataFrame(map(DoubleAggregateRDD.toRow), struct(Set("count", "result")))
  }
}

object DoubleAggregateRDD {
  def fromJava(row:QdbDoubleAggregation):DoubleAggregation = {
    DoubleAggregation(
      row.getCount,
      row.getResult.getValue)
  }

  def toRow(row:DoubleAggregation): Row = {
    Row(
      row.count,
      row.result)
  }
}
