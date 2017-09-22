package com.quasardb.spark.rdd.ts

import java.sql.Timestamp
import scala.collection.JavaConversions._
import org.apache.spark.rdd.RDD

import com.quasardb.spark.rdd.ts.DoubleRDD
import net.quasardb.qdb._

class DoubleRDDFunctions[A <: (Timestamp, Double)](data: RDD[A]) extends Serializable {

  def toQdbDoubleColumn(
    uri: String,
    table: String,
    column: String) : Unit = {

    println("storing partitions size = ", data.partitions.size)

    data.foreachPartition { partition =>
      var collection = new QdbDoubleColumnCollection(column)
      collection.addAll(partition.map(DoubleRDD.toJava).toList)

      new QdbCluster(uri).timeSeries(table).insertDoubles(collection)
    }
  }
}
