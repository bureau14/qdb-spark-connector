package com.quasardb.spark.rdd

import scala.collection.JavaConversions._

import org.apache.spark.rdd.RDD
import org.apache.spark._

import net.quasardb.qdb._


import com.quasardb.spark.partitioner._

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
