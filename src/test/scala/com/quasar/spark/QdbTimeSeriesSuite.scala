package net.quasardb.spark

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8
import java.util.Arrays
import java.sql.Timestamp
import java.time.{Instant, LocalDateTime}

import org.apache.spark.sql.{SQLContext, Row, SaveMode}
import org.apache.spark.{SparkContext, SparkException}

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.scalatest.Matchers._

import net.quasardb.qdb._;

import net.quasardb.spark._
import net.quasardb.spark.rdd._
import net.quasardb.spark.rdd.ts.{DoubleRDD, DoubleRDDFunctions, BlobRDD}

import scala.language.implicitConversions
import scala.collection.JavaConverters._
import scala.List
import scala.util.Random
import scala.sys.process._

class QdbTimeSeriesSuite extends FunSuite with BeforeAndAfterAll {

  private var qdbPort: Int = 2837
  private var qdbProc: Process = _
  private var defaultShardSize: Long = 1000 * 60 * 60 * 24 // 24 hours

  implicit val securityOptions : Option[QdbCluster.SecurityOptions] =
    Some(new QdbCluster.SecurityOptions("qdb-api-python",
      "SoHHpH26NtZvfq5pqm/8BXKbVIkf+yYiVZ5fQbq1nbcI=",
      "Pb+d1o3HuFtxEb5uTl9peU89ze9BZTK9f8KdKr4k7zGA="))

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
    val p = Process("qdb/bin/qdbd --cluster-private-file cluster-secret-key.txt --user-list users.txt -r " + dataRoot + " -a 127.0.0.1:" + qdbPort).run

    // :TODO: fix, proper 'wait for qdb to be alive'
    Thread.sleep(3000)
    p
  }

  private def destroyQdb(p:Process):Unit = {
    p.destroy

    // :TODO: fix, proper 'wait for qdb to be dead'
    Thread.sleep(3000)
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()


    qdbProc = launchQdb

    sqlContext = new SQLContext(new SparkContext("local[2]", "QdbTimeSeriesSuite"))

    // Create a timeseries table with random id
    table = java.util.UUID.randomUUID.toString
    doubleColumn = new QdbColumnDefinition.Double(java.util.UUID.randomUUID.toString)
    blobColumn = new QdbColumnDefinition.Blob(java.util.UUID.randomUUID.toString)

    val columns = List(doubleColumn, blobColumn)
    val series : QdbTimeSeries =
      Util.createCluster(qdbUri)
        .timeSeries(table)

    series.create(defaultShardSize, columns.asJava)

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
    val results = sqlContext
      .qdbDoubleColumn(qdbUri, table, doubleColumn.getName, doubleRanges)
      .collect()

    for (expected <- doubleCollection.asScala) {
      results should contain(DoubleRDD.fromJava(expected))
    }
  }

  test("all double data can be retrieved as a dataframe") {
    val results = sqlContext
      .qdbDoubleColumnAsDataFrame(qdbUri, table, doubleColumn.getName, doubleRanges)
      .collect()

    for (expected <- doubleCollection.asScala) {
      results should contain(DoubleRDD.toRow(DoubleRDD.fromJava(expected)))
    }
  }

  test("all double data can be aggregated as an RDD") {
    val results = sqlContext
      .qdbAggregateDoubleColumn(
        qdbUri,
        table,
        doubleColumn.getName,
        List(
          AggregateQuery(
            begin = Timestamp.valueOf(doubleCollection.range().getBegin().getValue()),
            end = Timestamp.valueOf(doubleCollection.range().getEnd().getValue().plusNanos(1)),
            operation = QdbAggregation.Type.COUNT)))
      .collect()

    results.length should equal(1)
    results.head.count should equal(doubleCollection.size())
  }

  test("all double data can be aggregated as a DataFrame") {
    val results = sqlContext
      .qdbAggregateDoubleColumnAsDataFrame(
        qdbUri,
        table,
        doubleColumn.getName,
        List(
          AggregateQuery(
            begin = Timestamp.valueOf(doubleCollection.range().getBegin().getValue()),
            end = Timestamp.valueOf(doubleCollection.range().getEnd().getValue().plusNanos(1)),
            operation = QdbAggregation.Type.COUNT)))
      .collect()

    results.length should equal(1)
    results.head.getLong(0) should equal(doubleCollection.size())
  }

  test("double data can be written in parallel as an RDD") {
    // Define a new table with only the double column as definition
    val newTable = java.util.UUID.randomUUID.toString
    val series : QdbTimeSeries =
      Util.createCluster(qdbUri)
        .timeSeries(newTable)
    val columns = List(doubleColumn)

    series.create(defaultShardSize, columns.asJava)

    val dataSet =
      doubleCollection.asScala.map(DoubleRDD.fromJava).toList

    sqlContext
      .sparkContext
      .parallelize(dataSet)
      .toQdbDoubleColumn(qdbUri, newTable, doubleColumn.getName)

    // Retrieve our test data
    val results = sqlContext
      .qdbDoubleColumn(qdbUri, newTable, doubleColumn.getName, doubleRanges)
      .collect()


    for (expected <- doubleCollection.asScala) {
      results should contain(DoubleRDD.fromJava(expected))
    }
  }

  test("double data can be copied as an RDD") {
    // Define a new table with only the double column as definition
    val newTable = java.util.UUID.randomUUID.toString
    val series : QdbTimeSeries =
      Util.createCluster(qdbUri)
        .timeSeries(newTable)
    val columns = List(doubleColumn)

    series.create(defaultShardSize, columns.asJava)

    sqlContext
      .qdbDoubleColumn(qdbUri, table, doubleColumn.getName, doubleRanges)
      .toQdbDoubleColumn(qdbUri, newTable, doubleColumn.getName)

    // Retrieve our test data
    val results = sqlContext
      .qdbDoubleColumn(qdbUri, newTable, doubleColumn.getName, doubleRanges)
      .collect()


    for (expected <- doubleCollection.asScala) {
      results should contain(DoubleRDD.fromJava(expected))
    }
  }

  test("double data can be copied as a dataframe") {
    // Define a new table with only the double column as definition
    val newTable = java.util.UUID.randomUUID.toString
    val series : QdbTimeSeries =
      Util.createCluster(qdbUri)
        .timeSeries(newTable)
    val columns = List(doubleColumn)

    series.create(defaultShardSize, columns.asJava)

    sqlContext
      .qdbDoubleColumnAsDataFrame(qdbUri, table, doubleColumn.getName, doubleRanges)
      .toQdbDoubleColumn(qdbUri, newTable, doubleColumn.getName)

    // Retrieve our test data
    val results = sqlContext
      .qdbDoubleColumnAsDataFrame(qdbUri, newTable, doubleColumn.getName, doubleRanges)
      .collect()

    doubleCollection
      .asScala
      .map(DoubleRDD.fromJava)
      .map(DoubleRDD.toRow)
      .foreach { expected =>
      results should contain(expected)
      }
  }

  /**
    * Blob tests
    */

  def hashBlobResult(x:Row):Row = {
    Row(
      x(0),
      Arrays.hashCode(x.get(1).asInstanceOf[Array[Byte]]))
  }

  def hashBlobResult(x:(Timestamp, Array[Byte])):(Timestamp, Int) = {
    (x._1, Arrays.hashCode(x._2))
  }


  test("all blob data can be retrieved as an RDD") {
    val results = sqlContext
      .qdbBlobColumn(qdbUri, table, blobColumn.getName, blobRanges)
      .collect()
      .map { x => hashBlobResult(x) }

    blobCollection
      .asScala
      .map(BlobRDD.fromJava)
      .map(hashBlobResult)
      .foreach { expected =>
        results should contain(expected)
      }
  }


  test("all blob data can be retrieved as a dataframe") {
    val df = sqlContext
      .qdbBlobColumnAsDataFrame(qdbUri, table, blobColumn.getName, blobRanges)

    val results = df
      .collect()
      .map {
        x => hashBlobResult(x)
      }

    blobCollection
      .asScala
      .map(BlobRDD.fromJava)
      .map(BlobRDD.toRow)
      .map(hashBlobResult)
      .foreach { expected =>
        results should contain(expected)
      }
  }

  test("all blob data can be aggregated as an RDD") {
    val results = sqlContext
      .qdbAggregateBlobColumn(
        qdbUri,
        table,
        blobColumn.getName,
        List(
          AggregateQuery(
            begin = Timestamp.valueOf(blobCollection.range().getBegin().getValue()),
            end = Timestamp.valueOf(blobCollection.range().getEnd().getValue().plusNanos(1)),
            operation = QdbAggregation.Type.COUNT)))
      .collect()

    results.length should equal(1)
    results.head.count should equal(blobCollection.size())
  }

  test("all blob data can be aggregated as a DataFrame") {
    val results = sqlContext
      .qdbAggregateBlobColumnAsDataFrame(
        qdbUri,
        table,
        blobColumn.getName,
        List(
          AggregateQuery(
            begin = Timestamp.valueOf(blobCollection.range().getBegin().getValue()),
            end = Timestamp.valueOf(blobCollection.range().getEnd().getValue().plusNanos(1)),
            operation = QdbAggregation.Type.COUNT)))
      .collect()

    results.length should equal(1)
    results.head.getLong(0) should equal(blobCollection.size())
  }

  test("blob data can be copied as an RDD") {
    // Define a new table with only the double column as definition
    val newTable = java.util.UUID.randomUUID.toString
    val series : QdbTimeSeries =
      Util.createCluster(qdbUri)
        .timeSeries(newTable)
    val columns = List(blobColumn)

    series.create(defaultShardSize, columns.asJava)

    sqlContext
      .qdbBlobColumn(qdbUri, table, blobColumn.getName, blobRanges)
      .toQdbBlobColumn(qdbUri, newTable, blobColumn.getName)

    // Retrieve our test data
    val results = sqlContext
      .qdbBlobColumn(qdbUri, newTable, blobColumn.getName, blobRanges)
      .collect()
      .map(hashBlobResult)

    blobCollection
      .asScala
      .map(BlobRDD.fromJava)
      .map(hashBlobResult)
      .foreach { expected =>
        results should contain(expected)
      }
  }

  test("blob data can be copied as a dataframe") {
    // Define a new table with only the blob column as definition
    val newTable = java.util.UUID.randomUUID.toString
    val series : QdbTimeSeries =
      Util.createCluster(qdbUri)
        .timeSeries(newTable)
    val columns = List(blobColumn)

    series.create(defaultShardSize, columns.asJava)

    sqlContext
      .qdbBlobColumnAsDataFrame(qdbUri, table, blobColumn.getName, blobRanges)
      .toQdbBlobColumn(qdbUri, newTable, blobColumn.getName)

    // Retrieve our test data
    val results = sqlContext
      .qdbBlobColumnAsDataFrame(qdbUri, newTable, blobColumn.getName, blobRanges)
      .collect()

    blobCollection
      .asScala
      .map(BlobRDD.fromJava)
      .map(BlobRDD.toRow)
      .foreach { expected =>
      results should contain(expected)
      }
  }
}
