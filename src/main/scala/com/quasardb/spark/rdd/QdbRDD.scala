package com.quasardb.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark._

import net.quasardb.qdb.QdbCluster;

import com.quasardb.spark.partitioner.QdbPartition

class QdbKeyRDD(sc: SparkContext,
                val uri: String)
    extends RDD[String](sc, Seq.empty) with Logging {

  override protected def getPartitions: Array[Partition] = List(new QdbPartition(0, uri)).toArray

  override def compute(
    split: Partition,
    context: TaskContext): Iterator[String] = {
    val partition: QdbPartition = split.asInstanceOf[QdbPartition]

    val qdb = new QdbCluster(uri)

    List("foo").iterator

  }
}
