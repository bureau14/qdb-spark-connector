package net.quasardb

import scala.language.implicitConversions

import java.sql.Timestamp

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import net.quasardb.spark.df._
import net.quasardb.spark.rdd._

import net.quasardb.qdb.Session;
import net.quasardb.qdb.ts.{WritableRow, TimeRange}

package object spark {

  implicit class QdbContext(@transient val sparkSession : SparkSession) {
    def tagAsDataFrame(
      uri: String,
      tag: String)(implicit securityOptions : Option[Session.SecurityOptions]) = {
      new QdbTagRDD(sparkSession.sparkContext, uri, tag)
    }

    def qdbDoubleColumn(
      uri: String,
      table: String,
      column: String,
      ranges: Array[TimeRange])(implicit securityOptions : Option[Session.SecurityOptions]) = {
      new DoubleRDD(sparkSession.sparkContext, uri, table, column, ranges)
    }

    def qdbTable(
      uri: String,
      table: String,
      ranges: Array[TimeRange])(implicit securityOptions : Option[Session.SecurityOptions]) = {
      new TableRDD(sparkSession.sparkContext, uri, table, ranges)
    }

    def qdbAggregateDoubleColumn(
      uri: String,
      table: String,
      columns: Seq[String],
      input: Seq[AggregateQuery])(implicit securityOptions : Option[Session.SecurityOptions])= {
      new DoubleAggregateRDD(sparkSession.sparkContext, uri, table, columns, input)
    }

    def qdbAggregateDoubleColumnAsDataFrame(
      uri: String,
      table: String,
      columns: Seq[String],
      input: Seq[AggregateQuery])(implicit securityOptions : Option[Session.SecurityOptions])= {
      qdbAggregateDoubleColumn(uri, table, columns, input)
        .toDataFrame(sparkSession)
    }

    def qdbDoubleColumnAsDataFrame(
      uri: String,
      table: String,
      column: String,
      ranges: Array[TimeRange])(implicit securityOptions : Option[Session.SecurityOptions]) = {
      qdbDoubleColumn(uri, table, column, ranges)
        .toDataFrame(sparkSession)
    }

    def qdbBlobColumn(
      uri: String,
      table: String,
      column: String,
      ranges: Array[TimeRange])(implicit securityOptions : Option[Session.SecurityOptions]) = {
      new BlobRDD(sparkSession.sparkContext, uri, table, column, ranges)
    }

    def qdbAggregateBlobColumn(
      uri: String,
      table: String,
      column: String,
      input: Seq[AggregateQuery])(implicit securityOptions : Option[Session.SecurityOptions]) = {
      new BlobAggregateRDD(sparkSession.sparkContext, uri, table, column, input)
    }

    def qdbAggregateBlobColumnAsDataFrame(
      uri: String,
      table: String,
      column: String,
      input: Seq[AggregateQuery])(implicit securityOptions : Option[Session.SecurityOptions]) = {
      qdbAggregateBlobColumn(uri, table, column, input)
        .toDataFrame(sparkSession)
    }

    def qdbBlobColumnAsDataFrame(
      uri: String,
      table: String,
      column: String,
      ranges: Array[TimeRange])(implicit securityOptions : Option[Session.SecurityOptions]) = {
      qdbBlobColumn(uri, table, column, ranges)
        .toDataFrame(sparkSession)
    }
  }

  implicit def toQdbTableRDDFunctions[A <: WritableRow](
    rdd: RDD[A]): TableRDDFunctions[A] = {
    return new TableRDDFunctions[A](rdd)
  }

  implicit def toQdbTableDataFrameFunctions(
    data: DataFrame): TableDataFrameFunctions = {
    return new TableDataFrameFunctions(data)
  }

  implicit def toQdbDoubleRDDFunctions[A <: (Timestamp, Double)](
    rdd: RDD[A]): DoubleRDDFunctions[A] = {
    return new DoubleRDDFunctions[A](rdd)
  }

  implicit def toQdbDoubleDataFrameFunctions(
    data: DataFrame): DoubleDataFrameFunctions = {
    return new DoubleDataFrameFunctions(data)
  }

  implicit def toQdbBlobRDDFunctions[A <: (Timestamp, Array[Byte])](
    rdd: RDD[A]): BlobRDDFunctions[A] = {
    return new BlobRDDFunctions[A](rdd)
  }

  implicit def toQdbBlobDataFrameFunctions(
    data: DataFrame): BlobDataFrameFunctions = {
    return new BlobDataFrameFunctions(data)
  }
}
