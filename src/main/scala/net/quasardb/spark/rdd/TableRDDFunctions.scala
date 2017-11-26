package net.quasardb.spark.rdd

import java.sql.Timestamp
import org.apache.spark.rdd.RDD

import net.quasardb.qdb._
import net.quasardb.spark.rdd.Util

class TableRDDFunctions[A <: QdbTimeSeriesRow](data: RDD[A]) extends Serializable {

  def toQdbTable(
    uri: String,
    table: String)(implicit securityOptions : Option[QdbSession.SecurityOptions]) : Unit = {

    data.foreachPartition { partition => Util.appendRows(uri, table, partition) }
  }
}
