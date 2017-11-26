package net.quasardb.spark.partitioner

import net.quasardb.qdb._
import org.apache.spark.Partition

object QdbPartitioner {

  def computePartitions(uri: String) : Array[Partition] = {
    /*
     TODO: partition based on some intelligent heuristic, such as
     the alias regions.
     */

    // val node : QdbNode = new QdbCluster(uri).node()
    // val topology : String = node.getNodeTopology(uri)


    List(new QdbPartition(0, uri)).toArray
  }

}
