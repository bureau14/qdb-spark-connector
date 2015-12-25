package com.quasardb.spark.partitioner

import java.net.InetAddress
import java.util
import org.apache.spark.Partition

case class QdbPartition(
  index: Int,
  uri: String) extends Partition
