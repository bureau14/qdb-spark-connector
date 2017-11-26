package net.quasardb.spark

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8
import java.util.Arrays
import java.sql.Timestamp
import java.time.{Instant, LocalDateTime, LocalTime, Duration}

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, Row, SaveMode}
import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.scalatest.Matchers._

import net.quasardb.qdb._;

import net.quasardb.spark._
import net.quasardb.spark.df._
import net.quasardb.spark.rdd._

import scala.language.implicitConversions
import scala.collection.JavaConverters._
import scala.List
import scala.util.Random
import scala.sys.process._

class QdbTimeSeriesSuite extends FunSuite with BeforeAndAfterAll {

  private var qdbPort: Int = 2837
  private var qdbProc: Process = _
  private var defaultShardSize: Long = 1000 * 60 * 60 * 24 // 24 hours

  implicit val securityOptions : Option[QdbSession.SecurityOptions] =
    Some(new QdbSession.SecurityOptions("qdb-api-python",
      "SoHHpH26NtZvfq5pqm/8BXKbVIkf+yYiVZ5fQbq1nbcI=",
      "Pb+d1o3HuFtxEb5uTl9peU89ze9BZTK9f8KdKr4k7zGA="))

  private val qdbUri: String = "qdb://127.0.0.1:" + qdbPort
  private val sparkContext: SparkContext = new SparkContext("local[2]", "QdbTimeSeriesSuite")
  private val sqlContext: SQLContext = new SQLContext(sparkContext)
  import sqlContext.implicits._

  private var table: String = _

  private var doubleColumn: QdbColumnDefinition = _
  private var doubleCollection: QdbDoubleColumnCollection = _
  private var doubleRanges: QdbTimeRangeCollection = new QdbTimeRangeCollection()

  private var blobColumn: QdbColumnDefinition = _
  private var blobCollection: QdbBlobColumnCollection = _
  private var blobRanges: QdbTimeRangeCollection = new QdbTimeRangeCollection

  private var testTable : Seq[QdbTimeSeriesRow] = _

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
        new QdbTimeSeriesRow(d.getTimestamp, Array(
          QdbTimeSeriesValue.createDouble(d.getValue),
          QdbTimeSeriesValue.createBlob(b.getValue))) }

    series.insertDoubles(doubleCollection)
    series.insertBlobs(blobCollection)

    val doubleRange = doubleCollection.range()
    doubleRanges.add(
      new QdbTimeRange(
        doubleRange.getBegin,
        new QdbTimespec(doubleRange.getEnd.asLocalDateTime.plusNanos(1))))


    val blobRange = blobCollection.range()
    blobRanges.add(
      new QdbTimeRange(
        blobRange.getBegin,
        new QdbTimespec(blobRange.getEnd.asLocalDateTime.plusNanos(1))))
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
            begin = doubleCollection.range().getBegin().asTimestamp(),
            end = Timestamp.valueOf(doubleCollection.range().getEnd().asLocalDateTime().plusNanos(1)),
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
            begin = blobCollection.range().getBegin().asTimestamp(),
            end = Timestamp.valueOf(blobCollection.range().getEnd().asLocalDateTime().plusNanos(1)),
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

  /**
    * Table tests
    */
  test("table data with doubles and blobs can be written in parallel as a DataFrame") {
    // Define a new table with only the double column as definition
    val newTable = java.util.UUID.randomUUID.toString
    val series : QdbTimeSeries =
      Util.createCluster(qdbUri)
        .timeSeries(newTable)

    val columns = List(doubleColumn, blobColumn)
    series.create(defaultShardSize, columns.asJava)

    val dataSet = testTable

    val schema = StructType(
      StructField("timestamp", TimestampType, true) ::
        StructField("column1", DoubleType, true) ::
        StructField("column2", BinaryType, true) :: Nil)

    val rdd : RDD[QdbTimeSeriesRow] = sqlContext
      .sparkContext
      .parallelize(dataSet)

    val df = sqlContext
      .createDataFrame(rdd.map(TableDataFrameFunctions.toRow), schema)
      .toQdbTable(qdbUri, newTable)


    // Retrieve our test data
    val doubleResults = sqlContext
      .qdbDoubleColumn(qdbUri, newTable, doubleColumn.getName, doubleRanges)
      .collect()
    val blobResults = sqlContext
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

  /**
    * Complex queries / benchmark / stresstest etc.
    */
  test("can do complex aggregations using DataFrame") {

    val startTime = LocalDateTime.of(2017,11,23,3,0)
    val points = (0 to 719).map { p => startTime.plusMinutes(p) }
    val sensors = List(java.util.UUID.randomUUID.toString, java.util.UUID.randomUUID.toString)
    val columns : List[QdbColumnDefinition] =
      List("temperature", "pressure", "volume", "weight", "colour",
        "latitude", "longitude", "altitude", "velocity").map { c => new QdbColumnDefinition.Double(c) }.toList
    val aggregations = List(QdbAggregation.Type.SUM, QdbAggregation.Type.ARITHMETIC_MEAN)

    // Ensure timeseries are created for each of our sensors
    sensors.foreach { s =>
      Util.createCluster(qdbUri)
        .timeSeries(s)
        .create(defaultShardSize, columns.asJava) }

    val r = scala.util.Random

    // Seed our timeseries sensors with random double data for each of the timepoints
    for (s <- sensors; c <- columns) {
      val doubleCollection = new QdbDoubleColumnCollection(c.getName)
      doubleCollection.addAll(
        points
          .map { new QdbTimespec(_) }
          .map { new QdbDoubleColumnValue(_, r.nextDouble)}
          .toList
          .filter(_ => r.nextBoolean) // randomly filter 50% of the data points
          .asJava)

      Util.createCluster(qdbUri)
        .timeSeries(s)
        .insertDoubles(doubleCollection)
    }

    // Now send aggregate requests per sensor, per column, per 5 minutes.
    // In order to do that, we first generate a List[Row] so that we can
    // create a dataframe out of that.
    val aggregatePoints =
      (0 to 719 by 5).map { p => startTime.plusMinutes(p) }

    // Now we can generate all our input dataframes which will be querying
    // quasardb.
    val inputDataFrames =
      for (s <- sensors; c <- columns)
        yield sqlContext
          .qdbAggregateDoubleColumnAsDataFrame(
            qdbUri,
            s,
            c.getName,
            (for (p <- aggregatePoints; a <- aggregations) yield {
              AggregateQuery(
                begin = Timestamp.valueOf(p),
                end = Timestamp.valueOf(p.plusMinutes(5)),
                operation = a)}).toList)

    // Before we can start actually processing our dataframes, let's define
    // our output schema which we will be coercing the last row to. This so
    // that Spark actually knows the labels and which types to expect.
    val defaultFields : List[StructField] =
      List(StructField("time series", StringType, true),
        StructField("start time", TimestampType, true),
        StructField("end time", TimestampType, true))
    val columnFields : List[StructField] =
      // Note that that we are hardcoding the format of the column here, which
      // is used in the lookup table in the Spark job.
      (for (c <- columns; a <- aggregations) yield {
          StructField(c.getName + " (" + a.toString + ")", DoubleType, true)
        }).toList
    val outputSchema =
      StructType(defaultFields ::: columnFields)
    val outputEncoder = RowEncoder(outputSchema)

    // Utility functon that is able to coerce 3 columns with aggregationtype, column id and value
    // into a single column with a zipped list.
    val zipper = udf[Seq[(String, String, Double)], Seq[String], Seq[String], Seq[Double]]((_, _, _).zipped.toList)

    val df = inputDataFrames
    // First step is to union all our dataframes into a single, big dataframe
      .reduceLeft((lhs, rhs) => lhs.unionAll(rhs))

    // Collect all relevant aggregate outputs per table (= sensor), per timespan
    .groupBy("table", "begin", "end")

    // Aggregate aggregation types, column id and the aggregation result into a
    // single column 'results'.
      .agg(collect_list(col("aggregationType")) as "aggregationTypes",
        collect_list(col("column")) as "columns",
        collect_list(col("result")) as "values")
        .withColumn("results", zipper(col("aggregationTypes"),col("columns"), col("values"))).drop("columns", "values", "aggregationTypes")

    // We only care about just a few columns, so let's get rid of all the noise data
      .select(
        col("table").as[String],
        col("begin").as[Timestamp],
        col("end").as[Timestamp],
        col("results").as[Seq[(String, String, Double)]])
      .map {
        case (table : String, begin : Timestamp, end : Timestamp, results : Seq[(String, String, Double)]) =>

          // Create a lookup map of column id -> value, so that we can
          // easily construct a single row from a sequence.
          val lookup = (Map(
            "time series" -> table,
            "start time" -> begin,
            "end time" -> end)
            ++
            // Here we hardcode the column name again, which is first defined in the
            // outputSchema's columnFields above.
            (results.map { case (x, y, z) => (y + " (" + x + ")", z) }.toMap))

          // Now all that's left is to simply look up each of our schema's columns
          // in our lookup table, or return null if it's not found.
          Row.fromSeq(
            outputSchema.map { x =>
              lookup.getOrElse(x.name, null)
            })
      } (outputEncoder)
      .sort(col("time series"), col("start time"))

    val results = df.collect()
    results.length should equal(aggregatePoints.length * sensors.length)
    results.foreach { row : Row =>
      sensors should contain(row.getAs[String]("time series"))
      aggregatePoints should contain(row.getAs[Timestamp]("start time").toLocalDateTime)
    }
  }
}
