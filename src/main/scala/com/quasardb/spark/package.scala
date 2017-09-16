package com.quasardb

import org.apache.spark.SparkContext
import com.quasardb.spark.rdd._

import net.quasardb.qdb.QdbTimeRangeCollection

package object spark {

  implicit class QdbContext(sc: SparkContext) {
    def fromQdbTag(uri: String,
                   tag: String) = {
      new QdbTagRDD(sc, uri, tag)
    }

    def fromQdbDoubleColumn(uri: String,
                            table: String,
                            column: String,
                            ranges: QdbTimeRangeCollection) = {
      new QdbTimeseriesDoubleRDD(sc, uri, table, column, ranges)
    }
  }

}
