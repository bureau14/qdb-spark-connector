package com.quasardb.spark.rdd.ts

import java.sql.Timestamp
import org.apache.spark.sql.DataFrame

import com.quasardb.spark.rdd.Util
import com.quasardb.spark.rdd.ts.BlobRDD

class BlobDataFrameFunctions(data: DataFrame) extends Serializable {

  def toQdbBlobColumn(
    uri: String,
    table: String,
    column: String) : Unit = {

    data
      .rdd
      .map(BlobRDD.fromRow)
      .foreachPartition { partition => Util.insertBlobs(uri, table, column, partition) }
  }
}
