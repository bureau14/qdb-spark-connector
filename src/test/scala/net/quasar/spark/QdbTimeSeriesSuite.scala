package net.quasardb.spark

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8
import java.util.Arrays
import java.sql.Timestamp
import java.time.{Instant, LocalDateTime, LocalTime, Duration}

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, Row => SparkRow, SaveMode}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.scalatest.Matchers._

import net.quasardb.qdb._;
import net.quasardb.qdb.ts.{WritableRow => QdbRow, Column, Value, Timespec, TimeRange, Table};

import net.quasardb.spark._
import net.quasardb.spark.df._
import net.quasardb.spark.rdd._

import scala.language.implicitConversions
import scala.collection.JavaConverters._
import scala.List
import scala.util.Random
import scala.sys.process._

class QdbTimeSeriesSuite extends FunSuite with BeforeAndAfterAll {

  private var qdbPort: Int = 28362
  private var qdbProc: Process = _
  private var defaultShardSize: Long = 1000 * 60 * 60 * 24 // 24 hours

  implicit val securityOptions : Option[Session.SecurityOptions] =
    Some(new Session.SecurityOptions("qdb-spark-connector",
      "SoHHpH26NtZvfq5pqm/8BXKbVIkf+yYiVZ5fQbq1nbcI=",
      "Pb+d1o3HuFtxEb5uTl9peU89ze9BZTK9f8KdKr4k7zGA="))

  private val qdbUri: String = "qdb://127.0.0.1:" + qdbPort
  private val sparkSession: SparkSession = SparkSession.builder()
    .master("local[2]")
    .appName("QdbTimeSeriesSuite")
    .getOrCreate()
  import sparkSession.implicits._

  private var table: String = _

  private var doubleColumn: Column = _
  private var doubleCollection: QdbDoubleColumnCollection = _
  private var doubleRanges: Array[TimeRange] = _

  private var blobColumn: Column = _
  private var blobCollection: QdbBlobColumnCollection = _
  private var blobRanges: Array[TimeRange] = _

  private var testTable : Seq[QdbRow] = _

  private def randomData(): ByteBuffer = {
    val str = java.util.UUID.randomUUID.toString
    var buf = ByteBuffer.allocateDirect(str.length)
    buf.put(str.getBytes("UTF-8"))
    buf.flip
    buf
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    // Create a timeseries table with random id
    table = java.util.UUID.randomUUID.toString
    doubleColumn = new Column.Double(java.util.UUID.randomUUID.toString)
    blobColumn = new Column.Blob(java.util.UUID.randomUUID.toString)

    val columns = Array(doubleColumn, blobColumn)
    val series : QdbTimeSeries =
      Util.createCluster(qdbUri)
        .timeSeries(table)

    series.create(defaultShardSize, columns)

    val r = scala.util.Random

    // Seed it with random doubles and blobs
    doubleCollection = new QdbDoubleColumnCollection(doubleColumn.getName())
    blobCollection = new QdbBlobColumnCollection(blobColumn.getName())

    doubleCollection.addAll(Seq.fill(100)(new QdbDoubleColumnValue(r.nextDouble)).toList.asJava)
    blobCollection.addAll(doubleCollection.asScala.map { d =>
      // This mapping ensures that the blob timestamps are exactly the same as
      // the double timestamps. This helps in building up the table data as well,
      // since then each stamp will have both double and blob data.
      new QdbBlobColumnValue(d.getTimestamp, randomData()) }.toList.asJava)

    // Seed our test table with both
    testTable = doubleCollection
      .asScala
      .zip(blobCollection.asScala)
      .map { case (d : QdbDoubleColumnValue, b : QdbBlobColumnValue) =>
        new QdbRow(d.getTimestamp, Array(
          Value.createDouble(d.getValue),
          Value.createBlob(b.getValue))) }

    series.insertDoubles(doubleCollection)
    series.insertBlobs(blobCollection)

    val doubleRange = doubleCollection.range()
    doubleRanges = Array(
      new TimeRange(
        doubleRange.getBegin,
        doubleRange.getEnd.plusNanos(1)))

    val blobRange = blobCollection.range()
    blobRanges = Array(
      new TimeRange(
        blobRange.getBegin,
        blobRange.getEnd.plusNanos(1)))
  }

  override protected def afterAll(): Unit = {
    try {
      sparkSession
        .sparkContext
        .stop()
        
      sparkSession
        .stop()

    } finally {
      super.afterAll()
    }
  }


  /**
    * Double tests
    */

  test("all double data can be retrieved as an RDD") {
    val results = sparkSession
      .qdbDoubleColumn(qdbUri, table, doubleColumn.getName, doubleRanges)
      .collect()

    for (expected <- doubleCollection.asScala) {
      results should contain(DoubleRDD.fromJava(expected))
    }
  }

  test("all double data can be retrieved as a dataframe") {
    val results = sparkSession
      .qdbDoubleColumnAsDataFrame(qdbUri, table, doubleColumn.getName, doubleRanges)
      .collect()

    for (expected <- doubleCollection.asScala) {
      results should contain(DoubleDataFrameFunctions.toRow(DoubleRDD.fromJava(expected)))
    }
  }

  test("all double data can be aggregated as an RDD") {
    val results = sparkSession
      .qdbAggregateDoubleColumn(
        qdbUri,
        table,
        List(doubleColumn.getName),
        List(
          AggregateQuery(
            begin = doubleCollection.range().getBegin().asTimestamp(),
            end = Timestamp.valueOf(doubleCollection.range().getEnd().asLocalDateTime().plusNanos(1)),
            operation = QdbAggregation.Type.COUNT)))
      .collect()

    results.length should equal(1)
    results.head.count should equal(doubleCollection.size())
  }

  test("all double data can be aggregated as a DataFrame") {
    val results = sparkSession
      .qdbAggregateDoubleColumnAsDataFrame(
        qdbUri,
        table,
        List(doubleColumn.getName),
        List(
          AggregateQuery(
            begin = doubleCollection.range().getBegin().asTimestamp(),
            end = Timestamp.valueOf(doubleCollection.range().getEnd().asLocalDateTime().plusNanos(1)),
            operation = QdbAggregation.Type.COUNT)))
      .collect()

    results.length should equal(1)
    results.head.getLong(5) should equal(doubleCollection.size())
  }

  test("double data can be written in parallel as an RDD") {
    // Define a new table with only the double column as definition
    val newTable = java.util.UUID.randomUUID.toString
    val series : QdbTimeSeries =
      Util.createCluster(qdbUri)
        .timeSeries(newTable)
    val columns = Array(doubleColumn)

    series.create(defaultShardSize, columns)

    val dataSet =
      doubleCollection.asScala.map(DoubleRDD.fromJava).toList

    sparkSession
      .sparkContext
      .parallelize(dataSet)
      .toQdbDoubleColumn(qdbUri, newTable, doubleColumn.getName)

    // Retrieve our test data
    val results = sparkSession
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
    val columns = Array(doubleColumn)

    series.create(defaultShardSize, columns)

    sparkSession
      .qdbDoubleColumn(qdbUri, table, doubleColumn.getName, doubleRanges)
      .toQdbDoubleColumn(qdbUri, newTable, doubleColumn.getName)

    // Retrieve our test data
    val results = sparkSession
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
    val columns = Array(doubleColumn)

    series.create(defaultShardSize, columns)

    sparkSession
      .qdbDoubleColumnAsDataFrame(qdbUri, table, doubleColumn.getName, doubleRanges)
      .toQdbDoubleColumn(qdbUri, newTable, doubleColumn.getName)

    // Retrieve our test data
    val results = sparkSession
      .qdbDoubleColumnAsDataFrame(qdbUri, newTable, doubleColumn.getName, doubleRanges)
      .collect()

    doubleCollection
      .asScala
      .map(DoubleRDD.fromJava)
      .map(DoubleDataFrameFunctions.toRow)
      .foreach { expected =>
      results should contain(expected)
      }
  }

  /**
    * Blob tests
    */

  def hashBlobResult(x:SparkRow):SparkRow = {
    SparkRow(
      x(0),
      Arrays.hashCode(x.get(1).asInstanceOf[Array[Byte]]))
  }

  def hashBlobResult(x:(Timestamp, Array[Byte])):(Timestamp, Int) = {
    (x._1, Arrays.hashCode(x._2))
  }


  test("all blob data can be retrieved as an RDD") {
    val results = sparkSession
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
    val df = sparkSession
      .qdbBlobColumnAsDataFrame(qdbUri, table, blobColumn.getName, blobRanges)

    val results = df
      .collect()
      .map {
        x => hashBlobResult(x)
      }

    blobCollection
      .asScala
      .map(BlobRDD.fromJava)
      .map(BlobDataFrameFunctions.toRow)
      .map(hashBlobResult)
      .foreach { expected =>
        results should contain(expected)
      }
  }

  test("all blob data can be aggregated as an RDD") {
    val results = sparkSession
      .qdbAggregateBlobColumn(
        qdbUri,
        table,
        blobColumn.getName,
        List(
          AggregateQuery(
            begin = blobCollection.range().getBegin().asTimestamp(),
            end = Timestamp.valueOf(blobCollection.range().getEnd().asLocalDateTime().plusNanos(1)),
            operation = QdbAggregation.Type.COUNT)))
      .collect()

    results.length should equal(1)
    results.head.count should equal(blobCollection.size())
  }

  test("all blob data can be aggregated as a DataFrame") {
    val results = sparkSession
      .qdbAggregateBlobColumnAsDataFrame(
        qdbUri,
        table,
        blobColumn.getName,
        List(
          AggregateQuery(
            begin = blobCollection.range().getBegin().asTimestamp(),
            end = Timestamp.valueOf(blobCollection.range().getEnd().asLocalDateTime().plusNanos(1)),
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
    val columns = Array(blobColumn)

    series.create(defaultShardSize, columns)

    sparkSession
      .qdbBlobColumn(qdbUri, table, blobColumn.getName, blobRanges)
      .toQdbBlobColumn(qdbUri, newTable, blobColumn.getName)

    // Retrieve our test data
    val results = sparkSession
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
    val columns = Array(blobColumn)

    series.create(defaultShardSize, columns)

    sparkSession
      .qdbBlobColumnAsDataFrame(qdbUri, table, blobColumn.getName, blobRanges)
      .toQdbBlobColumn(qdbUri, newTable, blobColumn.getName)

    // Retrieve our test data
    val results = sparkSession
      .qdbBlobColumnAsDataFrame(qdbUri, newTable, blobColumn.getName, blobRanges)
      .collect()

    blobCollection
      .asScala
      .map(BlobRDD.fromJava)
      .map(BlobDataFrameFunctions.toRow)
      .foreach { expected =>
      results should contain(expected)
      }
  }

  /**
    * Table tests
    */
  test("table data with doubles and blobs can be written in parallel as a DataFrame") {
    // Define a new table with only the double column as definition
    val newTable = java.util.UUID.randomUUID.toString
    val series : QdbTimeSeries =
      Util.createCluster(qdbUri)
        .timeSeries(newTable)

    val columns = Array(doubleColumn, blobColumn)
    series.create(defaultShardSize, columns)

    val dataSet = testTable

    val schema = StructType(
      StructField("timestamp", TimestampType, true) ::
        StructField("column1", DoubleType, true) ::
        StructField("column2", BinaryType, true) :: Nil)

    val rdd : RDD[QdbRow] = sparkSession
      .sparkContext
      .parallelize(dataSet)

    val df = sparkSession
      .createDataFrame(rdd.map(TableDataFrameFunctions.toRow), schema)
      .toQdbTable(qdbUri, newTable)


    // Retrieve our test data
    val doubleResults = sparkSession
      .qdbDoubleColumn(qdbUri, newTable, doubleColumn.getName, doubleRanges)
      .collect()
    val blobResults = sparkSession
      .qdbBlobColumn(qdbUri, newTable, blobColumn.getName, blobRanges)
      .collect()
      .map(hashBlobResult)

    doubleResults.length should equal(dataSet.length)
    blobResults.length should equal(dataSet.length)

    doubleCollection
      .asScala
      .foreach { d =>
        doubleResults should contain(DoubleRDD.fromJava(d))
      }

    blobCollection
      .asScala
      .map(BlobRDD.fromJava)
      .map(hashBlobResult)
      .foreach { expected =>
        blobResults should contain(expected)
      }
  }
}
