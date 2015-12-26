package com.quasardb

import org.apache.spark.SparkContext
import com.quasardb.spark.rdd._

package object spark {

  implicit class QdbContext(sc: SparkContext) {
    def fromQdbTag(uri: String,
                   tag: String) = {
      new QdbTagRDD(sc, uri, tag)
    }
  }

}
