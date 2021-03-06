package net.quasardb.spark

import java.nio.ByteBuffer

import org.apache.spark.sql.SparkSession

import org.scalatest.{BeforeAndAfterAll, FunSuite}

import net.quasardb.qdb._;
import net.quasardb.qdb.ts._;

import net.quasardb.spark._
import net.quasardb.spark.rdd._
import scala.sys.process._

import scala.language.implicitConversions

class QdbTagsSuite extends FunSuite with BeforeAndAfterAll {

  private val qdbPort: Int = 28360
  private var qdbUri: String = "qdb://127.0.0.1:" + qdbPort
  implicit val securityOptions : Option[Session.SecurityOptions] = None

  private var sparkSession: SparkSession = _
  private val key1: String = java.util.UUID.randomUUID.toString
  private val key2: String = java.util.UUID.randomUUID.toString
  private val key3: String = java.util.UUID.randomUUID.toString

  private val tag1: String = java.util.UUID.randomUUID.toString
  private val tag2: String = java.util.UUID.randomUUID.toString

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    sparkSession = SparkSession.builder()
      .master("local[2]")
      .appName("QdbTagsSuite")
      .getOrCreate()

    // Store a few default entries
    val entry1 = new QdbCluster(qdbUri).integer(key1)
    val entry2 = new QdbCluster(qdbUri).integer(key2)
    val entry3 = new QdbCluster(qdbUri).blob(key3)

    entry1.put(123)
    entry2.put(124)
    entry3.put(ByteBuffer.allocateDirect(3).put(new String("125").getBytes()))

    entry1.attachTag(tag1)
    entry2.attachTag(tag1)
    entry3.attachTag(tag2)
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

  test("searching for tags") {
    val results1 = sparkSession
      .tagAsDataFrame(qdbUri, tag1)
      .collect().sorted

    val results2 = sparkSession
      .tagAsDataFrame(qdbUri, tag2)
      .collect().sorted

    assert(results1.size == 2)

    assert(results1.head == key1 || results1.head == key2)
    assert(results1.last == key1 || results1.last == key2)

    assert(results2.size == 1)
    assert(results2.last == key3)
  }

  test("searching for integers by tag") {
    val results = sparkSession
      .tagAsDataFrame(qdbUri, tag1)
      .getInteger()
      .collect().sorted

    assert(results.size == 2)
    assert(results.head._2 == 123 || results.head._2 == 124)
    assert(results.last._2 == 123 || results.last._2 == 124)
  }

  test("searching for string by tag") {
    val results = sparkSession
      .tagAsDataFrame(qdbUri, tag2)
      .getString()
      .collect().sorted

    assert(results.size == 1)
    assert(results.last._2 == "125")
  }
}
