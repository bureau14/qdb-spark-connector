package net.quasardb.spark.df

import java.sql.Timestamp
import org.apache.spark.sql.DataFrame

import net.quasardb.qdb._
import net.quasardb.spark.rdd.Util
import net.quasardb.spark.rdd.TableRDD

class TableDataFrameFunctions(data: DataFrame) extends Serializable {

  def toQdbTable(
    uri: String,
    table: String)(implicit securityOptions : Option[QdbSession.SecurityOptions]) : Unit = {

    data
      .rdd
      .map(TableRDD.fromRow)
      .foreachPartition { partition => Util.appendRows(uri, table, partition) }
  }

  def byteBufferToByteArray(buf : ByteBuffer) : Array[Byte] = {
    var arr : Array[Byte] = new Array[Byte](buf.remaining)
    buf.get(arr)
    buf.rewind

    arr
  }

  def toRow(row:QdbTimeSeriesRow): Row = {
    val values : Array[Any] = row.getValues.map({ x : QdbTimeSeriesValue =>
      x.getType match {
        case QdbTimeSeriesValue.Type.DOUBLE => x.getDouble
        case QdbTimeSeriesValue.Type.BLOB => byteBufferToByteArray(x.getBlob)
      }
    })

    Row.fromSeq(Array(row.getTimestamp.asTimestamp) ++ values)
  }

  def fromRow(row:Row):QdbTimeSeriesRow = {
    println("converting row to qdb time series row: ", row.toString)
    val schema = row.schema

    new QdbTimeSeriesRow(
      row.getTimestamp(0),
      schema
        .fields
        .zipWithIndex
        .tail
        .map {
          case (StructField(name, dataType : DoubleType, nullable, metadata), index : Int) =>
            QdbTimeSeriesValue.createDouble(row.getAs[Double](index))
          case (StructField(name, dataType : BinaryType, nullable, metadata), index : Int) =>
            // Perhaps we do not need to create a safe blob here, but for the time
            // being let's be safe.
            QdbTimeSeriesValue.createSafeBlob(row.getAs[Array[Byte]](index))
        }

    )
  }

}
