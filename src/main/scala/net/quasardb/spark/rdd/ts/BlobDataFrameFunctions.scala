package net.quasardb.spark.rdd.ts

import java.sql.Timestamp
import org.apache.spark.sql.DataFrame

import net.quasardb.qdb._
import net.quasardb.spark.rdd.Util
import net.quasardb.spark.rdd.ts.BlobRDD

class BlobDataFrameFunctions(data: DataFrame) extends Serializable {

  def toQdbBlobColumn(
    uri: String,
    table: String,
    column: String)(implicit securityOptions : Option[QdbSession.SecurityOptions]) : Unit = {

    data
      .rdd
      .map(BlobRDD.fromRow)
      .foreachPartition { partition => Util.insertBlobs(uri, table, column, partition) }
  }
}
