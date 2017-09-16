package com.quasardb.spark

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8

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
  private var doubleRanges: QdbTimeRangeCollection = new QdbTimeRangeCollection()

  private var blobColumn: QdbColumnDefinition = _
  private var blobRanges: QdbTimeRangeCollection = new QdbTimeRangeCollection

  private def cleanQdb = {
    new QdbCluster(qdbUri).purgeAll(3000)
  }

  private def randomData(): ByteBuffer = {
    val str = java.util.UUID.randomUUID.toString
    var buf = ByteBuffer.allocateDirect(str.length)
    buf.put(str.getBytes("UTF-8"))
    buf.flip
    buf
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
    val series : QdbTimeSeries =
      new QdbCluster(qdbUri)
        .timeSeries(table)

    series.create(columns.asJava)

    val r = scala.util.Random
    // Seed it with random doubles and blobs

    val doubleCollection = new QdbDoubleColumnCollection(doubleColumn.getName())
    val blobCollection = new QdbBlobColumnCollection(blobColumn.getName())

    doubleCollection.addAll(Seq.fill(100)(new QdbDoubleColumnValue(r.nextDouble)).toList.asJava)
    blobCollection.addAll(Seq.fill(100)(new QdbBlobColumnValue(randomData())).toList.asJava)

    series.insertDoubles(doubleCollection)
    series.insertBlobs(blobCollection)

    val doubleRange = doubleCollection.range()
    doubleRanges.add(
      new QdbTimeRange(
        doubleRange.getBegin,
        new QdbTimespec(doubleRange.getEnd.getValue.plusNanos(1))))


    val blobRange = blobCollection.range()
    blobRanges.add(
      new QdbTimeRange(
        blobRange.getBegin,
        new QdbTimespec(blobRange.getEnd.getValue.plusNanos(1))))

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

  test("there is double data in the timeseries") {
    val results = sqlContext
      .sparkContext
      .fromQdbDoubleColumn(qdbUri, table, doubleColumn.getName, doubleRanges)
      .collect()

    for (result <- results) {
      println("result = ", result)
    }

    assert(results.size == 100)
  }

  test("there is blob data in the timeseries") {
    val results = sqlContext
      .sparkContext
      .fromQdbBlobColumn(qdbUri, table, blobColumn.getName, blobRanges)
      .collect()

    for (result <- results) {
      println("result = ", result)
    }

    assert(results.size == 100)
  }
}
