package net.quasardb.spark

import java.sql.Timestamp
import org.apache.spark.rdd.RDD

import net.quasardb.qdb._
import net.quasardb.spark.rdd._

class TableRDDFunctions[A <: QdbTimeSeriesRow](rdd: RDD[A]) extends Serializable {

  def toQdbTable(
    uri: String,
    table: String)(implicit securityOptions : Option[QdbSession.SecurityOptions]) : Unit = {

    rdd.foreachPartition { partition => Util.appendRows(uri, table, partition) }
  }

}

class DoubleRDDFunctions[A <: (Timestamp, Double)](rdd: RDD[A]) extends Serializable {

  def toQdbDoubleColumn(
    uri: String,
    table: String,
    column: String)
    (implicit securityOptions : Option[QdbSession.SecurityOptions],
      evidence: RDD[A] <:< RDD[(Timestamp, Double)]) : Unit = {
    rdd.foreachPartition { partition => Util.insertDoubles(uri, table, column, partition) }
  }
}


class BlobRDDFunctions[A <: (Timestamp, Array[Byte])](data: RDD[A]) extends Serializable {

  def toQdbBlobColumn(
    uri: String,
    table: String,
    column: String)(implicit securityOptions : Option[QdbSession.SecurityOptions]) : Unit = {

    data.foreachPartition { partition => Util.insertBlobs(uri, table, column, partition) }
  }
}
