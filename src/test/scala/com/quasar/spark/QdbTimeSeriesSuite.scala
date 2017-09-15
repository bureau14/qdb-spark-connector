package com.quasardb.spark

import java.nio.ByteBuffer

import org.apache.spark.sql.{SQLContext, Row, SaveMode}
import org.apache.spark.{SparkContext, SparkException}

import org.scalatest.{BeforeAndAfterAll, FunSuite}

import net.quasardb.qdb._;

import com.quasardb.spark._
import com.quasardb.spark.rdd._

import scala.language.implicitConversions
import scala.collection.JavaConverters._
import scala.List
import scala.util.Random

class QdbTimeSeriesSuite extends FunSuite with BeforeAndAfterAll {

  private var qdbUri: String = "qdb://127.0.0.1:2836"
  private var sqlContext: SQLContext = _
  private var table: String = _
  private var doubleColumn: QdbColumnDefinition = _
  private var blobColumn: QdbColumnDefinition = _

  private def cleanQdb = {
    new QdbCluster(qdbUri).purgeAll(3000)
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    sqlContext = new SQLContext(new SparkContext("local[2]", "QdbTimeSeriesSuite"))
    cleanQdb

    // Create a timeseries table with random id
    table = java.util.UUID.randomUUID.toString
    doubleColumn = new QdbColumnDefinition.Double(java.util.UUID.randomUUID.toString)
    blobColumn = new QdbColumnDefinition.Blob(java.util.UUID.randomUUID.toString)

    val columns = List(doubleColumn, blobColumn)
    new QdbCluster(qdbUri)
      .timeSeries(table)
      .create(columns.asJava)

    val r = scala.util.Random
    // Seed it with random doubles and blobs
    val doubles = 0 to 100 foreach { _ =>
      new QdbDoubleColumnValue(r.nextDouble)
    }
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

  test("world domination") {
    assert(true == false)
  }
}
