package net.quasardb.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, Row, DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark._

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8
import java.sql.Timestamp

import net.quasardb.qdb._
import net.quasardb.qdb.ts.TimeRange

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

import net.quasardb.spark.df.DoubleDataFrameFunctions
import net.quasardb.spark.partitioner._
// import net.quasardb.spark.rdd.Util

class DoubleRDD(
  sc: SparkContext,
  val uri: String,
  val table: String,
  val column: String,
  val ranges: Array[TimeRange])(implicit securityOptions : Option[Session.SecurityOptions])
    extends RDD[(Timestamp, Double)](sc, Nil) {

  override protected def getPartitions = QdbPartitioner.computePartitions(uri)

  override def compute(
    split: Partition,
    context: TaskContext): Iterator[(Timestamp, Double)] = {
    val partition: QdbPartition = split.asInstanceOf[QdbPartition]

    val series: QdbTimeSeries = Util.createCluster(partition.uri).timeSeries(table)

    // TODO: limit query to only the Partition
    series.getDoubles(column, ranges).toList.map(DoubleRDD.fromJava).iterator
  }

  def toDataFrame(sqlContext: SQLContext): DataFrame = {
    val struct =
      StructType(
        StructField("timestamp", TimestampType, false) ::
        StructField("value", DoubleType, false) :: Nil)

    sqlContext.createDataFrame(map(DoubleDataFrameFunctions.toRow), struct(Set("timestamp", "value")))
  }
}

object DoubleRDD {
  def fromJava(row:QdbDoubleColumnValue):(Timestamp, Double) = {
    (row.getTimestamp.asTimestamp, row.getValue)
  }

  def toJava(row:(Timestamp, Double)):QdbDoubleColumnValue = {
    new QdbDoubleColumnValue(row._1, row._2)
  }
}
