package com.quasardb.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark._
import java.nio.ByteBuffer

import net.quasardb.qdb._

import scala.collection.JavaConversions._

import com.quasardb.spark.partitioner.QdbPartition

class QdbStringRDD(prev: RDD[String])
    extends RDD[(String, String)](prev) with Logging {

  override protected def getPartitions: Array[Partition] = prev.partitions

  override def compute(
    split: Partition,
    context: TaskContext): Iterator[(String, String)] = {
    val partition: QdbPartition = split.asInstanceOf[QdbPartition]
    val keys = firstParent[String].iterator(split, context)

    val cluster = new QdbCluster(partition.uri)

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

  def getString(): RDD[(String, String)] = {
    new QdbStringRDD(this)
  }

  def getInteger(): RDD[(String, Long)] = {
    new QdbIntegerRDD(this)
  }

}
