package com.quasardb.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark._
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8

import net.quasardb.qdb._

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

import com.quasardb.spark.partitioner._

class QdbStringRDD(prev: RDD[String])
    extends RDD[(String, String)](prev) {

  override protected def getPartitions: Array[Partition] = prev.partitions

  override def compute(
    split: Partition,
    context: TaskContext): Iterator[(String, String)] = {
    val partition: QdbPartition = split.asInstanceOf[QdbPartition]
    val keys = firstParent[String].iterator(split, context)

    val cluster = new QdbCluster(partition.uri)

    // TODO: limit query to only the Partition

    keys.map(key => {
      val buffer = cluster.blob(key).get()

      (key, UTF_8.decode(buffer.toByteBuffer).toString())
    })
  }
}

class QdbIntegerRDD(prev: RDD[String])
    extends RDD[(String, Long)](prev) {

  override protected def getPartitions = prev.partitions

  override def compute(
    split: Partition,
    context: TaskContext): Iterator[(String, Long)] = {
    val partition: QdbPartition = split.asInstanceOf[QdbPartition]
    val keys = firstParent[String].iterator(split, context)

    val cluster = new QdbCluster(partition.uri)

    // TODO: limit query to only the Partition

    keys.map(key =>
      (key, cluster.integer(key).get()))
  }
}

class QdbTagRDD(
  sc: SparkContext,
  val uri: String,
  val tag: String)
    extends RDD[String](sc, Seq.empty) {

  override protected def getPartitions = QdbPartitioner.computePartitions(uri)

  override def compute(
    split: Partition,
    context: TaskContext): Iterator[String] = {
    val partition: QdbPartition = split.asInstanceOf[QdbPartition]

    // TODO: limit query to only the Partition
    new QdbCluster(partition.uri).tag(tag).entries().toList.map(x => x.alias).iterator
  }

  def getString(): RDD[(String, String)] = {
    new QdbStringRDD(this)
  }

  def getInteger(): RDD[(String, Long)] = {
    new QdbIntegerRDD(this)
  }
}

class QdbTimeseriesDoubleRDD(
  sc: SparkContext,
  val uri: String,
  val table: String,
  val column: String,
  val ranges: QdbTimeRangeCollection)
    extends RDD[QdbDoubleColumnValue](sc, Seq.empty) {

  override protected def getPartitions = QdbPartitioner.computePartitions(uri)

  override def compute(
    split: Partition,
    context: TaskContext): Iterator[QdbDoubleColumnValue] = {
    val partition: QdbPartition = split.asInstanceOf[QdbPartition]

    val series: QdbTimeSeries = new QdbCluster(partition.uri).timeSeries(table)

    // TODO: limit query to only the Partition
    series.getDoubles(column, ranges).toList.iterator
  }
}

class QdbTimeseriesBlobRDD(
  sc: SparkContext,
  val uri: String,
  val table: String,
  val column: String,
  val ranges: QdbTimeRangeCollection)
    extends RDD[QdbBlobColumnValue](sc, Seq.empty) {

  override protected def getPartitions = QdbPartitioner.computePartitions(uri)

  override def compute(
    split: Partition,
    context: TaskContext): Iterator[QdbBlobColumnValue] = {
    val partition: QdbPartition = split.asInstanceOf[QdbPartition]

    val series: QdbTimeSeries = new QdbCluster(partition.uri).timeSeries(table)

    // TODO: limit query to only the Partition
    series.getBlobs(column, ranges).toList.iterator
  }
}
