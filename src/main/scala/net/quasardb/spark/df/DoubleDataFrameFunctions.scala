package net.quasardb.spark.df

import java.sql.Timestamp
import org.apache.spark.sql.{SQLContext, Row, DataFrame}

import net.quasardb.qdb._
import net.quasardb.spark.rdd.Util
import net.quasardb.spark.rdd.DoubleRDD

class DoubleDataFrameFunctions(data: DataFrame) extends Serializable {

  def toQdbDoubleColumn(
    uri: String,
    table: String,
    column: String)(implicit securityOptions : Option[QdbSession.SecurityOptions]) : Unit = {

    data
      .rdd
      .map(DoubleDataFrameFunctions.fromRow)
      .foreachPartition { partition => Util.insertDoubles(uri, table, column, partition) }
  }
}


object DoubleDataFrameFunctions {
  def fromRow(row:Row):(Timestamp, Double) = {
    (row.getTimestamp(0), row.getDouble(1))
  }

  def toRow(row:(Timestamp, Double)): Row = {
    Row(row._1, row._2)
  }
}
