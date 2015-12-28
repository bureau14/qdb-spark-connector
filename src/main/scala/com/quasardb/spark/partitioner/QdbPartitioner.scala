package com.quasardb.spark.partitioner

import net.quasardb.qdb._
import org.apache.spark.Partition

object QdbPartitioner {

  def computePartitions(uri: String) : Array[Partition] = {
    val topology : String = new QdbCluster(uri).getNodeTopology(uri)

    /* 
     TODO: partition based on some intelligent heuristic, such as
     the alias regions.
     */

    List(new QdbPartition(0, uri)).toArray
  }

}
