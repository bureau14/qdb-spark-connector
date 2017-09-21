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

    def qdbDoubleColumn(uri: String,
                        table: String,
                        column: String,
                        ranges: QdbTimeRangeCollection) = {
      new QdbTimeSeriesDoubleRDD(sqlContext.sparkContext, uri, table, column, ranges)
    }

    def qdbDoubleColumnAsDataFrame(uri: String,
                                   table: String,
                                   column: String,
                                   ranges: QdbTimeRangeCollection) = {
      qdbDoubleColumn(uri, table, column, ranges)
        .toDataFrame(sqlContext)
    }

    def qdbBlobColumn(uri: String,
                      table: String,
                      column: String,
                      ranges: QdbTimeRangeCollection) = {
      new QdbTimeSeriesBlobRDD(sqlContext.sparkContext, uri, table, column, ranges)
    }

    def qdbBlobColumnAsDataframe(uri: String,
                                 table: String,
                                 column: String,
                                 ranges: QdbTimeRangeCollection) = {
      qdbBlobColumn(uri, table, column, ranges)
        .toDataFrame(sqlContext)
    }
  }
}
