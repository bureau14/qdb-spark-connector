package net.quasardb.spark.rdd

import scala.collection.JavaConversions._

import org.apache.spark.rdd.RDD
import org.apache.spark._

import net.quasardb.qdb._


import net.quasardb.spark.rdd.Util
import net.quasardb.spark.partitioner._

class QdbIntegerRDD(prev: RDD[String])(implicit securityOptions : Option[QdbCluster.SecurityOptions])
    extends RDD[(String, Long)](prev) {

  override protected def getPartitions = prev.partitions

  override def compute(
    split: Partition,
    context: TaskContext): Iterator[(String, Long)] = {
    val partition: QdbPartition = split.asInstanceOf[QdbPartition]
    val keys = firstParent[String].iterator(split, context)

    val cluster = Util.createCluster(partition.uri)

    // TODO: limit query to only the Partition

    keys.map(key =>
      (key, cluster.integer(key).get()))
  }
}
