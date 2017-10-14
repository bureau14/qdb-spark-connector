package net.quasardb.spark.rdd

import scala.collection.JavaConversions._
import org.apache.spark.rdd.RDD
import org.apache.spark._

import net.quasardb.qdb._


import net.quasardb.spark.partitioner._
import net.quasardb.spark.rdd.Util

class QdbTagRDD(
  sc: SparkContext,
  val uri: String,
  val tag: String)(implicit securityOptions : Option[QdbCluster.SecurityOptions])
    extends RDD[String](sc, Nil) {

  override protected def getPartitions = QdbPartitioner.computePartitions(uri)

  override def compute(
    split: Partition,
    context: TaskContext): Iterator[String] = {
    val partition: QdbPartition = split.asInstanceOf[QdbPartition]

    // TODO: limit query to only the Partition
    Util.createCluster(partition.uri).tag(tag).entries().toList.map(x => x.alias).iterator
  }

  def getString(): RDD[(String, String)] = {
    new QdbStringRDD(this)
  }

  def getInteger(): RDD[(String, Long)] = {
    new QdbIntegerRDD(this)
  }
}
