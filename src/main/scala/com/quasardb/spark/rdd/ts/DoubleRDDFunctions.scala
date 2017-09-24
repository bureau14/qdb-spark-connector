package com.quasardb.spark.rdd.ts

import java.sql.Timestamp
import org.apache.spark.rdd.RDD
import com.quasardb.spark.rdd.Util

class DoubleRDDFunctions[A <: (Timestamp, Double)](data: RDD[A]) extends Serializable {

  def toQdbDoubleColumn(
    uri: String,
    table: String,
    column: String) : Unit = {

    data.foreachPartition { partition => Util.insertDoubles(uri, table, column, partition) }
  }
}
