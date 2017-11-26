package net.quasardb.spark.df

import java.sql.Timestamp
import org.apache.spark.sql.DataFrame

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
      .map(DoubleRDD.fromRow)
      .foreachPartition { partition => Util.insertDoubles(uri, table, column, partition) }
  }
}
