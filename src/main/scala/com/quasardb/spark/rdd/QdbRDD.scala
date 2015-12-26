package com.quasardb.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark._
import java.nio.ByteBuffer

import net.quasardb.qdb._

import scala.collection.JavaConversions._

import com.quasardb.spark.partitioner.QdbPartition

class QdbBlobRDD(prev: RDD[String])
    extends RDD[(String, ByteBuffer)](prev) with Logging {

  override protected def getPartitions: Array[Partition] = prev.partitions

  override def compute(
    split: Partition,
    context: TaskContext): Iterator[(String, ByteBuffer)] = {
    val partition: QdbPartition = split.asInstanceOf[QdbPartition]
    val keys = firstParent[String].iterator(split, context)

    val cluster = new QdbCluster(partition.uri)

    keys.map(key =>
      (key, cluster.getBlob(key).get()))
  }
}

class QdbIntegerRDD(prev: RDD[String])
    extends RDD[(String, Long)](prev) with Logging {

  override protected def getPartitions: Array[Partition] = prev.partitions

  override def compute(
    split: Partition,
    context: TaskContext): Iterator[(String, Long)] = {
    val partition: QdbPartition = split.asInstanceOf[QdbPartition]
    val keys = firstParent[String].iterator(split, context)

    val cluster = new QdbCluster(partition.uri)

    keys.map(key =>
      (key, cluster.getInteger(key).get()))
  }
}

class QdbTagRDD(sc: SparkContext,
                val uri: String,
                val tag: String)
    extends RDD[String](sc, Seq.empty) with Logging {

  override protected def getPartitions: Array[Partition] = List(new QdbPartition(0, uri)).toArray

  override def compute(
    split: Partition,
    context: TaskContext): Iterator[String] = {
    val partition: QdbPartition = split.asInstanceOf[QdbPartition]

    new QdbCluster(partition.uri).getTag(tag).getEntries().toList.iterator
  }

  def getBlob(): RDD[(String, ByteBuffer)] = {
    new QdbBlobRDD(this)
  }

  def getInteger(): RDD[(String, Long)] = {
    new QdbIntegerRDD(this)
  }

}
