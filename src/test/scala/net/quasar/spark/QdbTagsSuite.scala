package net.quasardb.spark

import java.nio.ByteBuffer

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext}

import org.scalatest.{BeforeAndAfterAll, FunSuite}

import net.quasardb.qdb._;
import net.quasardb.qdb.ts._;

import net.quasardb.spark._
import net.quasardb.spark.rdd._
import scala.sys.process._

import scala.language.implicitConversions

class QdbTagsSuite extends FunSuite with BeforeAndAfterAll {

  private val qdbPort: Int = 2838
  private var qdbProc: Process = _
  private var qdbUri: String = "qdb://127.0.0.1:" + qdbPort
  implicit val securityOptions : Option[Session.SecurityOptions] = None

  private var sqlContext: SQLContext = _
  private val key1: String = java.util.UUID.randomUUID.toString
  private val key2: String = java.util.UUID.randomUUID.toString
  private val key3: String = java.util.UUID.randomUUID.toString

  private val tag1: String = java.util.UUID.randomUUID.toString
  private val tag2: String = java.util.UUID.randomUUID.toString

  private def launchQdb():Process = {
    val dataRoot = java.nio.file.Files.createTempDirectory(java.util.UUID.randomUUID.toString).toString
    val p = Process("qdb/bin/qdbd --security 0 -r " + dataRoot + " -a 127.0.0.1:" + qdbPort).run

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
    sqlContext = new SQLContext(new SparkContext("local[2]", "QdbTagsSuite"))

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
      sqlContext
        .sparkContext
        .stop()

      destroyQdb(qdbProc)

    } finally {
      super.afterAll()
    }
  }

  test("searching for tags") {
    val results1 = sqlContext
      .tagAsDataFrame(qdbUri, tag1)
      .collect().sorted

    val results2 = sqlContext
      .tagAsDataFrame(qdbUri, tag2)
      .collect().sorted

    assert(results1.size == 2)

    assert(results1.head == key1 || results1.head == key2)
    assert(results1.last == key1 || results1.last == key2)

    assert(results2.size == 1)
    assert(results2.last == key3)
  }

  test("searching for integers by tag") {
    val results = sqlContext
      .tagAsDataFrame(qdbUri, tag1)
      .getInteger()
      .collect().sorted

    assert(results.size == 2)
    assert(results.head._2 == 123 || results.head._2 == 124)
    assert(results.last._2 == 123 || results.last._2 == 124)
  }

  test("searching for string by tag") {
    val results = sqlContext
      .tagAsDataFrame(qdbUri, tag2)
      .getString()
      .collect().sorted

    assert(results.size == 1)
    assert(results.last._2 == "125")
  }
}
