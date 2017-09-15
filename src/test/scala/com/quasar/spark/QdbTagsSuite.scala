package com.quasardb.spark

import java.nio.ByteBuffer

import org.apache.spark.sql.{SQLContext, Row, SaveMode}
import org.apache.spark.{SparkContext, SparkException}

import org.scalatest.{BeforeAndAfterAll, FunSuite}

import net.quasardb.qdb.QdbCluster;

import com.quasardb.spark._
import com.quasardb.spark.rdd._

import scala.language.implicitConversions

class QdbTagsSuite extends FunSuite with BeforeAndAfterAll {

  private var qdbUri: String = "qdb://127.0.0.1:2836"
  private var sqlContext: SQLContext = _

  private def cleanQdb = {
    new QdbCluster(qdbUri).purgeAll(3000)
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    sqlContext = new SQLContext(new SparkContext("local[2]", "QdbTagsSuite"))

    cleanQdb

    // Store a few default entries
    val entry1 = new QdbCluster(qdbUri).integer("key1")
    val entry2 = new QdbCluster(qdbUri).integer("key2")
    val entry3 = new QdbCluster(qdbUri).blob("key3")

    entry1.put(123)
    entry2.put(124)
    entry3.put(ByteBuffer.allocateDirect(3).put(new String("125").getBytes()))

    entry1.attachTag("tag1")
    entry2.attachTag("tag1")
    entry3.attachTag("tag2")
  }

  override protected def afterAll(): Unit = {
    try {
      sqlContext
        .sparkContext
        .stop()

      cleanQdb

    } finally {
      super.afterAll()
    }
  }

  test("searching for tags") {
    val results1 = sqlContext
      .sparkContext
      .fromQdbTag(qdbUri, "tag1")
      .collect().sorted

    val results2 = sqlContext
      .sparkContext
      .fromQdbTag(qdbUri, "tag2")
      .collect().sorted

    assert(results1.size == 2)

    assert(results1.head == "key1")
    assert(results1.last == "key2")

    assert(results2.size == 1)
    assert(results2.last == "key3")
  }

  test("searching for integers by tag") {
    val results = sqlContext
      .sparkContext
      .fromQdbTag(qdbUri, "tag1")
      .getInteger()
      .collect().sorted

    assert(results.size == 2)
    assert(results.head._2 == 123)
    assert(results.last._2 == 124)
  }

  test("searching for string by tag") {
    val results = sqlContext
      .sparkContext
      .fromQdbTag(qdbUri, "tag2")
      .getString()
      .collect().sorted

    assert(results.size == 1)
    assert(results.last._2 == "125")
  }

}
