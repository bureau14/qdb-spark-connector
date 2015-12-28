package com.quasardb.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark._
import java.nio.ByteBuffer

import net.quasardb.qdb._

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

import com.quasardb.spark.partitioner._

class QdbStringRDD(prev: RDD[String])
    extends RDD[(String, String)](prev) with Logging {

  override protected def getPartitions: Array[Partition] = prev.partitions

  override def compute(
    split: Partition,
    context: TaskContext): Iterator[(String, String)] = {
    val partition: QdbPartition = split.asInstanceOf[QdbPartition]
    val keys = firstParent[String].iterator(split, context)

    val cluster = new QdbCluster(partition.uri)

    // TODO: limit query to only the Partition

    keys.map(key => {
      val buffer = cluster.getBlob(key).get()
      val bytes = Array.fill[Byte](buffer.limit())(0)
      buffer.get(bytes)

      (key, new String(bytes))
    })
  }
}

class QdbIntegerRDD(prev: RDD[String])
    extends RDD[(String, Long)](prev) with Logging {

  override protected def getPartitions = prev.partitions

  override def compute(
    split: Partition,
    context: TaskContext): Iterator[(String, Long)] = {
    val partition: QdbPartition = split.asInstanceOf[QdbPartition]
    val keys = firstParent[String].iterator(split, context)

    val cluster = new QdbCluster(partition.uri)

    // TODO: limit query to only the Partition

    keys.map(key =>
      (key, cluster.getInteger(key).get()))
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
    new QdbCluster(partition.uri).getTag(tag).getEntries().toList.iterator
  }

  def getString(): RDD[(String, String)] = {
    new QdbStringRDD(this)
  }

  def getInteger(): RDD[(String, Long)] = {
    new QdbIntegerRDD(this)
  }
}
