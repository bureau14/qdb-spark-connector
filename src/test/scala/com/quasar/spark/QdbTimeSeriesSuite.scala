package com.quasardb.spark

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8
import java.sql.Timestamp
import java.time.{Instant, LocalDateTime}

import org.apache.spark.sql.{SQLContext, Row, SaveMode}
import org.apache.spark.{SparkContext, SparkException}

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.scalatest.Matchers._

import net.quasardb.qdb._;

import com.quasardb.spark._
import com.quasardb.spark.rdd._
import com.quasardb.spark.rdd.ts.{DoubleRDD, DoubleRDDFunctions, BlobRDD}

import scala.language.implicitConversions
import scala.collection.JavaConverters._
import scala.List
import scala.util.Random
import scala.sys.process._

class QdbTimeSeriesSuite extends FunSuite with BeforeAndAfterAll {

  private var qdbPort: Int = 2837
  private var qdbProc: Process = _
  private var qdbUri: String = "qdb://127.0.0.1:" + qdbPort
  private var sqlContext: SQLContext = _
  private var table: String = _

  private var doubleColumn: QdbColumnDefinition = _
  private var doubleCollection: QdbDoubleColumnCollection = _
  private var doubleRanges: QdbTimeRangeCollection = new QdbTimeRangeCollection()

  private var blobColumn: QdbColumnDefinition = _
  private var blobCollection: QdbBlobColumnCollection = _
  private var blobRanges: QdbTimeRangeCollection = new QdbTimeRangeCollection

  private def randomData(): ByteBuffer = {
    val str = java.util.UUID.randomUUID.toString
    var buf = ByteBuffer.allocateDirect(str.length)
    buf.put(str.getBytes("UTF-8"))
    buf.flip
    buf
  }

  private def launchQdb():Process = {
    val dataRoot = java.nio.file.Files.createTempDirectory(java.util.UUID.randomUUID.toString).toString
    val p = Process("qdb/bin/qdbd --security 0 -r " + dataRoot + " -a 127.0.0.1:" + qdbPort).run

    // :TODO: fix, proper 'wait for qdb to be alive'
    Thread.sleep(500)
    p
  }

  private def destroyQdb(p:Process):Unit = {
    p.destroy

    // :TODO: fix, proper 'wait for qdb to be dead'
    Thread.sleep(500)
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()


    println("beforeAll for timeseries..")
    qdbProc = launchQdb

    sqlContext = new SQLContext(new SparkContext("local[2]", "QdbTimeSeriesSuite"))

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

    doubleCollection = new QdbDoubleColumnCollection(doubleColumn.getName())
    blobCollection = new QdbBlobColumnCollection(blobColumn.getName())

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

      destroyQdb(qdbProc)

    } finally {
      super.afterAll()
    }
  }


  /**
    * Double tests
    */

  test("all double data can be retrieved as an RDD") {
    println("1 in test for timeseries..")

    val results = sqlContext
      .qdbDoubleColumn(qdbUri, table, doubleColumn.getName, doubleRanges)
      .collect()

    println("2 in test for timeseries..")

    for (expected <- doubleCollection.asScala) {
      results should contain(DoubleRDD.fromJava(expected))
    }

    println("3 in test for timeseries..")
  }

  // test("all double data can be retrieved as a dataframe") {
  //   val results = sqlContext
  //     .qdbDoubleColumnAsDataFrame(qdbUri, table, doubleColumn.getName, doubleRanges)
  //     .collect()

  //   for (expected <- doubleCollection.asScala) {
  //     results should contain(DoubleRDD.toRow(DoubleRDD.fromJava(expected)))
  //   }
  // }

  // test("double data can be written as an RDD") {
  //   // Define a new table with only the double column as definition
  //   val newTable = java.util.UUID.randomUUID.toString
  //   val series : QdbTimeSeries =
  //     new QdbCluster(qdbUri)
  //       .timeSeries(newTable)
  //   val columns = List(doubleColumn)

  //   series.create(columns.asJava)

  //   val dataSet = doubleCollection.asScala.map(DoubleRDD.fromJava).toList

  //   sqlContext
  //     .sparkContext
  //     .parallelize(dataSet)
  //     .toQdbDoubleColumn(qdbUri, newTable, doubleColumn.getName)

  //   // Retrieve our test data
  //   val results = sqlContext
  //     .qdbDoubleColumn(qdbUri, newTable, doubleColumn.getName, doubleRanges)
  //     .collect()


  //   for (expected <- doubleCollection.asScala) {
  //     results should contain(DoubleRDD.fromJava(expected))
  //   }
  // }

  // test("double data can be written as a dataframe") {

  //   // Define a new table with only the double column as definition
  //   val newTable = java.util.UUID.randomUUID.toString
  //   val series : QdbTimeSeries =
  //     new QdbCluster(qdbUri)
  //       .timeSeries(newTable)
  //   val columns = List(doubleColumn)

  //   series.create(columns.asJava)

  //   // Write our test data based on our input test data
  //   val input = sqlContext
  //     .qdbDoubleColumnAsDataFrame(qdbUri, table, doubleColumn.getName, doubleRanges)
  //     .collect()
  //   println("saved to qdb")

  //   // sqlContext
  //   //   .sparkContext
  //   //   .parallelize(dataSet)
  //   //   .saveToQdbDoubleColumn(table, column)


  //   // Retrieve our test data

  //   val results = sqlContext
  //     .qdbDoubleColumnAsDataFrame(qdbUri, newTable, doubleColumn.getName, doubleRanges)
  //     .collect()

  //   for (expected <- doubleCollection.asScala) {
  //     results should contain(DoubleRDD.toRow(expected))
  //   }
  // }

  /**
    * Blob tests
  //   */

  // test("all blob data can be retrieved as an RDD") {
  //   val results = sqlContext
  //     .qdbBlobColumn(qdbUri, table, blobColumn.getName, blobRanges).collect()

  //   for (expected <- blobCollection.asScala) {
  //     results should contain(expected)
  //   }
  // }

  // test("all blob data can be retrieved as a dataframe") {
  //   val df = sqlContext
  //     .qdbBlobColumnAsDataframe(qdbUri, table, blobColumn.getName, blobRanges)

  //   val results = df.collect()

  //   for (expected <- blobCollection.asScala) {
  //     results should contain(BlobRDD.toRow(expected))
  //   }
  // }
}
