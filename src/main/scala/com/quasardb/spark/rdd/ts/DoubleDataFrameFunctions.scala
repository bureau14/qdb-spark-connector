package com.quasardb.spark.rdd.ts

import java.sql.Timestamp
import org.apache.spark.sql.DataFrame

import com.quasardb.spark.rdd.Util
import com.quasardb.spark.rdd.ts.DoubleRDD

class DoubleDataFrameFunctions(data: DataFrame) extends Serializable {

  def toQdbDoubleColumn(
    uri: String,
    table: String,
    column: String) : Unit = {

    data
      .rdd
      .map(DoubleRDD.fromRow)
      .foreachPartition { partition => Util.insertDoubles(uri, table, column, partition) }
  }
}
