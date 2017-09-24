package com.quasardb.spark.rdd.ts

import java.sql.Timestamp
import org.apache.spark.rdd.RDD
import com.quasardb.spark.rdd.Util

class BlobRDDFunctions[A <: (Timestamp, Array[Byte])](data: RDD[A]) extends Serializable {

  def toQdbBlobColumn(
    uri: String,
    table: String,
    column: String) : Unit = {

    data.foreachPartition { partition => Util.insertBlobs(uri, table, column, partition) }
  }
}
