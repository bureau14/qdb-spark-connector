package net.quasardb.spark.rdd

import java.sql.Timestamp
import org.apache.spark.rdd.RDD

import net.quasardb.qdb._
import net.quasardb.spark.rdd.Util

class DoubleRDDFunctions[A <: (Timestamp, Double)](data: RDD[A]) extends Serializable {

  def toQdbDoubleColumn(
    uri: String,
    table: String,
    column: String)(implicit securityOptions : Option[QdbSession.SecurityOptions]) : Unit = {

    data.foreachPartition { partition => Util.insertDoubles(uri, table, column, partition) }
  }
}
