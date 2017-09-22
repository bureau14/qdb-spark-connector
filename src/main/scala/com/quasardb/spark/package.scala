package com.quasardb

import java.sql.Timestamp

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

import com.quasardb.spark.rdd._
import com.quasardb.spark.rdd.ts.{DoubleRDD, DoubleRDDFunctions}

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
      new DoubleRDD(sqlContext.sparkContext, uri, table, column, ranges)
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

  implicit def toQdbDoubleRDDFunctions[A <: (Timestamp, Double)](rdd: RDD[A]): DoubleRDDFunctions[A] = {
    return new DoubleRDDFunctions[A](rdd)
  }
}
