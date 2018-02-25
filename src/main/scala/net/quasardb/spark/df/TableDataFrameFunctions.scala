package net.quasardb.spark.df

import java.nio.ByteBuffer
import java.sql.Timestamp

import org.apache.spark.sql.{SQLContext, Row => SparkRow, DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.sql._

import net.quasardb.qdb._
import net.quasardb.qdb.ts.{Row => QdbRow, Value}
import net.quasardb.spark.rdd.Util
import net.quasardb.spark.rdd.TableRDD

class TableDataFrameFunctions(data: DataFrame) extends Serializable {

  def toQdbTable(
    uri: String,
    table: String)(implicit securityOptions : Option[Session.SecurityOptions]) : Unit = {

    data
      .rdd
      .map(TableDataFrameFunctions.fromRow)
      .foreachPartition { partition => Util.appendRows(uri, table, partition) }
  }
}

object TableDataFrameFunctions {
  def byteBufferToByteArray(buf : ByteBuffer) : Array[Byte] = {
    var arr : Array[Byte] = new Array[Byte](buf.remaining)
    buf.get(arr)
    buf.rewind

    arr
  }

  def toRow(row:QdbRow): SparkRow = {
    val values : Array[Any] = row.getValues.map({ x : Value =>
      x.getType match {
        case Value.Type.DOUBLE => x.getDouble
        case Value.Type.BLOB => byteBufferToByteArray(x.getBlob)
      }
    })

    SparkRow.fromSeq(Array(row.getTimestamp.asTimestamp) ++ values)
  }

  def fromRow(row:SparkRow):QdbRow = {
    println("converting row to qdb time series row: ", row.toString)
    val schema = row.schema

    new QdbRow(
      row.getTimestamp(0),
      schema
        .fields
        .zipWithIndex
        .tail
        .map {
          case (StructField(name, dataType : DoubleType, nullable, metadata), index : Int) =>
            Value.createDouble(row.getAs[Double](index))
          case (StructField(name, dataType : BinaryType, nullable, metadata), index : Int) =>
            // Perhaps we do not need to create a safe blob here, but for the time
            // being let's be safe.
            Value.createSafeBlob(row.getAs[Array[Byte]](index))
        }

    )
  }

}
