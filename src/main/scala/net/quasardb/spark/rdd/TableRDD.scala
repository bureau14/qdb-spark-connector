package net.quasardb.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark._

import java.nio.charset.StandardCharsets.UTF_8
import java.sql.Timestamp

import net.quasardb.qdb.Session
import net.quasardb.qdb.ts.{WritableRow, TimeRange}

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

import net.quasardb.spark.partitioner._
import net.quasardb.spark.df.TableDataFrameFunctions

class TableRDD(
  sc: SparkContext,
  val uri: String,
  val table: String,
  val ranges: Array[TimeRange])(implicit securityOptions : Option[Session.SecurityOptions])
    extends RDD[Row](sc, Nil) {

  override protected def getPartitions = QdbPartitioner.computePartitions(uri)

  override def compute(
    split: Partition,
    context: TaskContext): Iterator[Row] = {
    // Not implemented yet
    val emptyList : List[Row] = List()
    emptyList.iterator
  }

  def toDataFrame(sparkSession: SparkSession): DataFrame = {
    val struct =
      StructType(
        StructField("timestamp", TimestampType, false) ::
        StructField("value", DoubleType, false) :: Nil)

    sparkSession.createDataFrame(map(TableDataFrameFunctions.toRow), struct(Set("timestamp", "value")))
  }
}
