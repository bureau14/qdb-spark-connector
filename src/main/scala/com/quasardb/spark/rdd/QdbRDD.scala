package com.quasardb.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark._

import net.quasardb.qdb.QdbCluster;
import net.quasardb.qdb.QdbEntry;

import scala.collection.JavaConversions._

import com.quasardb.spark.partitioner.QdbPartition

class QdbTagRDD(sc: SparkContext,
                val uri: String,
                val tag: String)
    extends RDD[String](sc, Seq.empty) with Logging {

  override protected def getPartitions: Array[Partition] = List(new QdbPartition(0, uri)).toArray

  override def compute(
    split: Partition,
    context: TaskContext): Iterator[String] = {
    val partition: QdbPartition = split.asInstanceOf[QdbPartition]

    new QdbCluster(uri).getTag(tag).getEntries().toList.iterator
  }
}
