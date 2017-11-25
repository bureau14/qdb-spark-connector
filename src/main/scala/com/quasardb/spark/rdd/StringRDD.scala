package net.quasardb.spark.rdd

import java.nio.charset.StandardCharsets.UTF_8
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, Row, DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark._

import net.quasardb.qdb._

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

import net.quasardb.spark.rdd.Util
import net.quasardb.spark.partitioner._

class QdbStringRDD(prev: RDD[String])(implicit securityOptions : Option[QdbSession.SecurityOptions])
    extends RDD[(String, String)](prev) {

  override protected def getPartitions: Array[Partition] = prev.partitions

  override def compute(
    split: Partition,
    context: TaskContext): Iterator[(String, String)] = {
    val partition: QdbPartition = split.asInstanceOf[QdbPartition]
    val keys = firstParent[String].iterator(split, context)

    val cluster = Util.createCluster(partition.uri)

    // TODO: limit query to only the Partition

    keys.map(key => {
      val buffer = cluster.blob(key).get()

      (key, UTF_8.decode(buffer.toByteBuffer).toString())
    })
  }
}
