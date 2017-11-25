package net.quasardb

import java.sql.Timestamp

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

import net.quasardb.spark.rdd._
import net.quasardb.spark.rdd.ts._

import net.quasardb.qdb.{QdbSession, QdbTimeRangeCollection, QdbTimeSeriesRow};

package object spark {

  implicit class QdbContext(@transient val sqlContext : SQLContext) {
    def tagAsDataFrame(
      uri: String,
      tag: String)(implicit securityOptions : Option[QdbSession.SecurityOptions]) = {
      new QdbTagRDD(sqlContext.sparkContext, uri, tag)
    }

    def qdbDoubleColumn(
      uri: String,
      table: String,
      column: String,
      ranges: QdbTimeRangeCollection)(implicit securityOptions : Option[QdbSession.SecurityOptions]) = {
      new DoubleRDD(sqlContext.sparkContext, uri, table, column, ranges)
    }

    def qdbTable(
      uri: String,
      table: String,
      ranges: QdbTimeRangeCollection)(implicit securityOptions : Option[QdbSession.SecurityOptions]) = {
      new TableRDD(sqlContext.sparkContext, uri, table, ranges)
    }

    def qdbAggregateDoubleColumn(
      uri: String,
      table: String,
      column: String,
      input: Seq[AggregateQuery])(implicit securityOptions : Option[QdbSession.SecurityOptions])= {
      new DoubleAggregateRDD(sqlContext.sparkContext, uri, table, column, input)
    }

    def qdbAggregateDoubleColumnAsDataFrame(
      uri: String,
      table: String,
      column: String,
      input: Seq[AggregateQuery])(implicit securityOptions : Option[QdbSession.SecurityOptions])= {
      qdbAggregateDoubleColumn(uri, table, column, input)
        .toDataFrame(sqlContext)
    }

    def qdbDoubleColumnAsDataFrame(
      uri: String,
      table: String,
      column: String,
      ranges: QdbTimeRangeCollection)(implicit securityOptions : Option[QdbSession.SecurityOptions]) = {
      qdbDoubleColumn(uri, table, column, ranges)
        .toDataFrame(sqlContext)
    }

    def qdbBlobColumn(
      uri: String,
      table: String,
      column: String,
      ranges: QdbTimeRangeCollection)(implicit securityOptions : Option[QdbSession.SecurityOptions]) = {
      new BlobRDD(sqlContext.sparkContext, uri, table, column, ranges)
    }

    def qdbAggregateBlobColumn(
      uri: String,
      table: String,
      column: String,
      input: Seq[AggregateQuery])(implicit securityOptions : Option[QdbSession.SecurityOptions]) = {
      new BlobAggregateRDD(sqlContext.sparkContext, uri, table, column, input)
    }

    def qdbAggregateBlobColumnAsDataFrame(
      uri: String,
      table: String,
      column: String,
      input: Seq[AggregateQuery])(implicit securityOptions : Option[QdbSession.SecurityOptions]) = {
      qdbAggregateBlobColumn(uri, table, column, input)
        .toDataFrame(sqlContext)
    }

    def qdbBlobColumnAsDataFrame(
      uri: String,
      table: String,
      column: String,
      ranges: QdbTimeRangeCollection)(implicit securityOptions : Option[QdbSession.SecurityOptions]) = {
      qdbBlobColumn(uri, table, column, ranges)
        .toDataFrame(sqlContext)
    }
  }

  implicit def toQdbDoubleRDDFunctions[A <: (Timestamp, Double)](
    rdd: RDD[A]): DoubleRDDFunctions[A] = {
    return new DoubleRDDFunctions[A](rdd)
  }

  implicit def toQdbTableRDDFunctions[A <: QdbTimeSeriesRow](
    rdd: RDD[A]): TableRDDFunctions[A] = {
    return new TableRDDFunctions[A](rdd)
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
