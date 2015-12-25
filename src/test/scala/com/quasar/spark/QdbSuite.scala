package com.quasardb.spark

import org.apache.spark.sql.{SQLContext, Row, SaveMode}
import org.apache.spark.{SparkContext, SparkException}

import org.scalatest.{BeforeAndAfterAll, FunSuite}

import com.quasardb.spark._
import com.quasardb.spark.rdd._

import scala.language.implicitConversions

class QdbSuite extends FunSuite with BeforeAndAfterAll {

  private var sqlContext: SQLContext = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    sqlContext = new SQLContext(new SparkContext("local[2]", "QdbSuite"))
  }

  override protected def afterAll(): Unit = {
    try {
      sqlContext
        .sparkContext
        .stop()
    } finally {
      super.afterAll()
    }
  }

  test("creating RDD") {
    val results = sqlContext
      .sparkContext
      .fromQdbUri("qdb://127.0.0.1:2836")
      .collect()

    assert(results.size == 1)
    assert(results.last == "foo")

  }
}
