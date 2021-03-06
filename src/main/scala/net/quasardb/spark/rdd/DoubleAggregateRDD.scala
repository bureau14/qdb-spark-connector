package net.quasardb.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, Row, DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark._

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8
import java.sql.Timestamp

import net.quasardb.qdb._
import net.quasardb.qdb.ts.{TimeRange, Timespec}

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

// import net.quasardb.spark.rdd.{AggregateQuery, Util}
import net.quasardb.spark.partitioner._

case class DoubleAggregation(
  table: String,
  column: String,
  aggreggationType: String,
  begin: Timestamp,
  end: Timestamp,
  count: Long,
  result: Double
) extends Serializable

class DoubleAggregateRDD(
  sc: SparkContext,
  val uri: String,
  val table: String,
  val columns: Seq[String],
  val input: Seq[AggregateQuery])(implicit securityOptions : Option[Session.SecurityOptions])
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
            new TimeRange(
              new Timespec(begin),
              new Timespec(end))))
      }
    }

    val partition: QdbPartition = split.asInstanceOf[QdbPartition]
    val series: QdbTimeSeries = Util.createCluster(partition.uri).timeSeries(table)

    // TODO: limit query to only the Partition

    columns.map { column =>
      series
        .doubleAggregate(column, aggregate)
        .toList
        .map(DoubleAggregateRDD.fromJava(table, column)) }
      .flatten
      .iterator
  }

  def toDataFrame(sparkSession: SparkSession): DataFrame = {
    val struct =
      StructType(
        StructField("table", StringType, true) ::
        StructField("column", StringType, true) ::
        StructField("aggregationType", StringType, true) ::
        StructField("begin", TimestampType, true) ::
        StructField("end", TimestampType, true) ::
        StructField("count", LongType, true) ::
        StructField("result", DoubleType, true) :: Nil)

    sparkSession.createDataFrame(map(DoubleAggregateRDD.toRow), struct(Set("table", "column", "aggregationType", "begin", "end", "count", "result")))
  }
}

object DoubleAggregateRDD {
  def fromJava(table:String, column:String)(row:QdbDoubleAggregation):DoubleAggregation = {
    DoubleAggregation(
      table,
      column,
      row.getType.toString,
      row.getRange.getBegin.asTimestamp,
      row.getRange.getEnd.asTimestamp,
      row.getCount,
      row.getResult.getValue)
  }

  def toRow(row : DoubleAggregation): Row = {
    Row(
      row.table,
      row.column,
      row.aggreggationType,
      row.begin,
      row.end,
      row.count,
      row.result)
  }
}
