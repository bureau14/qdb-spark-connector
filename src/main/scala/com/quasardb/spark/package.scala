package com.quasardb

import org.apache.spark.sql.{DataFrame, SQLContext}
import com.quasardb.spark.rdd._

import net.quasardb.qdb.QdbTimeRangeCollection

package object spark {

  implicit class QdbContext(@transient val sqlContext : SQLContext) {
    def tagAsDataFrame(uri: String,
                       tag: String) = {
      new QdbTagRDD(sqlContext.sparkContext, uri, tag)
    }

    def qdbDoubleColumnAsDataFrame(uri: String,
                                   table: String,
                                   column: String,
                                   ranges: QdbTimeRangeCollection) = {
      new QdbTimeSeriesDoubleRDD(sqlContext.sparkContext, uri, table, column, ranges)
        .toDataFrame(sqlContext)
    }

    def qdbBlobColumnAsDataframe(uri: String,
                                 table: String,
                                 column: String,
                                 ranges: QdbTimeRangeCollection) = {
      new QdbTimeSeriesBlobRDD(sqlContext.sparkContext, uri, table, column, ranges)
        .toDataFrame(sqlContext)
    }
  }
}
