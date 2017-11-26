package net.quasardb.spark.df

import java.sql.Timestamp
import org.apache.spark.sql.{SQLContext, Row, DataFrame}

import net.quasardb.qdb._
import net.quasardb.spark.rdd.Util
import net.quasardb.spark.rdd.BlobRDD

class BlobDataFrameFunctions(data: DataFrame) extends Serializable {

  def toQdbBlobColumn(
    uri: String,
    table: String,
    column: String)(implicit securityOptions : Option[QdbSession.SecurityOptions]) : Unit = {

    data
      .rdd
      .map(BlobDataFrameFunctions.fromRow)
      .foreachPartition { partition => Util.insertBlobs(uri, table, column, partition) }
  }
}


object BlobDataFrameFunctions {
  def fromRow(row:Row):(Timestamp, Array[Byte]) = {
    (row.getTimestamp(0), row.getAs[Array[Byte]](1))
  }

  def toRow(row:(Timestamp, Array[Byte])): Row = {
    Row(row._1, row._2)
  }
}
