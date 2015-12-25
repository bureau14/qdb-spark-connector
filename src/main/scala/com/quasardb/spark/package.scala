package com.quasardb

import org.apache.spark.SparkContext
import com.quasardb.spark.rdd._

package object spark {

  implicit class QdbContext(sc: SparkContext) {
    def fromQdbUri(uri: String) = {
      new QdbKeyRDD(sc, uri)
    }
  }

}
