package com.quasardb.spark

import org.apache.spark.sql.{SQLContext, Row, SaveMode}
import org.apache.spark.{SparkContext, SparkException}

import org.scalatest.{BeforeAndAfterAll, FunSuite}

import net.quasardb.qdb.QdbCluster;

import com.quasardb.spark._
import com.quasardb.spark.rdd._

import scala.language.implicitConversions

class QdbSuite extends FunSuite with BeforeAndAfterAll {

  private var qdbUri: String = "qdb://127.0.0.1:2836"
  private var sqlContext: SQLContext = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    sqlContext = new SQLContext(new SparkContext("local[2]", "QdbSuite"))

    // Store a few default entries
    val entry1 = new QdbCluster(qdbUri).getInteger("key1")
    val entry2 = new QdbCluster(qdbUri).getInteger("key2")
    val entry3 = new QdbCluster(qdbUri).getInteger("key3")

    entry1.put(123)
    entry2.put(124)
    entry3.put(125)

    entry1.addTag("tag1")
    entry2.addTag("tag1")
    entry3.addTag("tag2")
  }

  override protected def afterAll(): Unit = {
    try {
      sqlContext
        .sparkContext
        .stop()

      new QdbCluster(qdbUri).removeEntry("key1")
      new QdbCluster(qdbUri).removeEntry("key2")
      new QdbCluster(qdbUri).removeEntry("key3")

    } finally {
      super.afterAll()
    }
  }

  test("creating RDD") {
    val results = sqlContext
      .sparkContext
      .fromQdbTag(qdbUri, "tag1")
      .collect().sorted

    assert(results.size == 2)

    assert(results.head == "key1")
    assert(results.last == "key2")
  }
}
