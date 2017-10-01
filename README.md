# qdb-spark-connector
Official quasardb Spark connector. It extends quasardb's support to allow storing and retrieving data as Spark RDDs or DataFrames.

# Querying QuasarDB

Given a QuasarDB timeseries table `doubles_test` that looks as follows

| timestamp           | value     |
| ------------------- | --------- |
| 2017-10-01 12:09:03 | 1.2345678 |
| 2017-10-01 12:09:04 | 8.7654321 |
| 2017-10-01 12:09:05 | 5.6789012 |
| 2017-10-01 12:09:06 | 2.1098765 |

## Querying using RDD

The `qdbDoubleColumn` is an implicit method on an RDD[(Timestamp, Double)], and the `qdbBlobColumn` is an implicit method on an RDD[(Timestamp, Array[Byte])). Both methods require explicitly passing a `qdbUri`, a `tableName`, a `columnName` and a `QdbTimeRangeCollection` as demonstrated below.

```scala
import java.sql.Timestamp
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import com.quasardb.spark._
import net.quasardb.qdb.QdbTimeRangeCollection

val qdbUri = "qdb://127.0.01:2836"
val timeRanges = new QdbTimeRangeCollection
timeRanges.add(
  new QdbTimeRange(
    Timestamp.valueOf("2017-10-01 12:09:03"),
    Timestamp.valueOf("2017-10-01 12:09:07")))
    
val sc = new SparkContext("local", "qdb-test")
val sqlContext = new SQLContext(sc)

val rdd = sqlContext
  .qdbDoubleColumn(
    qdbUri,
    "doubles_test",
    "value",
    timeRanges)
  .collect
```

## Querying using a DataFrame

The `qdbDoubleColumnAsDataFrame` and the `qdbBlobColumnAsDataFrame` allows querying data from QuasarDB directly as a DataFrame. It exposes a DataFrame with the columns `timestamp` and `value`, where the value corresponds to the value type of the data being queried (`Double` or `Array[Byte]`).

```scala
import java.sql.Timestamp
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import com.quasardb.spark._
import net.quasardb.qdb.QdbTimeRangeCollection

val qdbUri = "qdb://127.0.01:2836"
val timeRanges = new QdbTimeRangeCollection
timeRanges.add(
  new QdbTimeRange(
    Timestamp.valueOf("2017-10-01 12:09:03"),
    Timestamp.valueOf("2017-10-01 12:09:07")))
    
val sc = new SparkContext("local", "qdb-test")
val sqlContext = new SQLContext(sc)

val df = sqlContext
  .qdbDoubleColumnAsDataFrame(
    qdbUri,
    "doubles_test",
    "dbl_value",
    timeRanges)
  .collect
```

## Aggregating using an RDD

QuasarDB exposes its native aggregation capabilities as RDD using the implicit methods `qdbAggregateDoubleColumn` and `qdbAggregateBlobColumn` that requires passing a `qdbUri`, `tableName`, `columnName` and a sequence of `AggregateQuery`. It returns exactly one result row for each `AggregateQuery` provided.

```scala
import java.sql.Timestamp
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import net.quasardb.qdb.QdbAggregation
import com.quasardb.spark._
import com.quasardb.spark.rdd.AggregateQuery

val qdbUri = "qdb://127.0.01:2836"
val sc = new SparkContext("local", "qdb-test")
val sqlContext = new SQLContext(sc)

val results = sqlContext
  .qdbAggregateDoubleColumn(
    qdbUri,
    "doubles_test",
    "value",
    List(
      AggregateQuery(
        begin = Timestamp.valueOf("2017-10-01 12:09:03"),
        end = = Timestamp.valueOf("2017-10-01 12:09:07"),
        operation = QdbAggregation.Type.COUNT))        
  .collect

results.length should equal(1)
results.head.count should equal(doubleCollection.size)
```

## Aggregating using a DataFrame

QuasarDB exposes its native aggregation capabilities as DataFrame using the implicit methods `qdbAggregateDoubleColumnAsDataFrame` and `qdbAggregateBlobColumnAsDataFrame` that requires passing a `qdbUri`, `tableName`, `columnName` and a sequence of `AggregateQuery`. It returns exactly one result row for each `AggregateQuery` provided.

```scala
import java.sql.Timestamp
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import net.quasardb.qdb.QdbAggregation
import com.quasardb.spark._
import com.quasardb.spark.rdd.AggregateQuery

val qdbUri = "qdb://127.0.01:2836"
val sc = new SparkContext("local", "qdb-test")
val sqlContext = new SQLContext(sc)

val results = sqlContext
  .qdbAggregateDoubleColumnAsDataFrame(
    qdbUri,
    "doubles_test",
    "value",
    List(
      AggregateQuery(
        begin = Timestamp.valueOf("2017-10-01 12:09:03"),
        end = = Timestamp.valueOf("2017-10-01 12:09:07"),
        operation = QdbAggregation.Type.COUNT))        
  .collect

results.length should equal(1)
results.head.getLong(0) should equal(doubleCollection.size)
```

# Writing to QuasarDB

## Writing using an RDD

```scala
import java.sql.Timestamp
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import com.quasardb.spark._

val qdbUri = "qdb://127.0.01:2836"    
val sc = new SparkContext("local", "qdb-test")
val sqlContext = new SQLContext(sc)

val dataSet = List(
  (Timestamp.valueOf("2017-10-01 12:09:03"), 1.2345678),
  (Timestamp.valueOf("2017-10-01 12:09:04"), 8.7654321),
  (Timestamp.valueOf("2017-10-01 12:09:05"), 5.6789012),
  (Timestamp.valueOf("2017-10-01 12:09:06"), 2.1098765))

sc
  .parallelize(dataSet)
  .toQdbDoubleColumn(qdbUri, "doubles_test", "dbl_value")
```
## Writing using a DataFrame

The `qdbDoubleColumnAsDataFrame` and `qdbBlobColumnAsDataFrame` functions are available in the Spark SQLContext to allow storing data into QuasarDB. You must also pass a `qdbUri`, a `tableName` and a `columnName` to the function to store the data in the correct location.

The code example below copies all the data from the `doubles_test` table into the `doubles_test_copy` table.

```scala
import java.sql.Timestamp
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import com.quasardb.spark._
import net.quasardb.qdb.QdbTimeRangeCollection

val qdbUri = "qdb://127.0.01:2836"
val timeRanges = new QdbTimeRangeCollection
timeRanges.add(
  new QdbTimeRange(
    Timestamp.valueOf("2017-10-01 12:09:03"),
    Timestamp.valueOf("2017-10-01 12:09:07")))
    
val sc = new SparkContext("local", "qdb-test")
val sqlContext = new SQLContext(sc)

val df = sqlContext
  .qdbDoubleColumnAsDataFrame(
    qdbUri,
    "doubles_test",
    "dbl_value",
    timeRanges)
  .toQdbDoubleColumn(qdbUri, "doubles_test_copy", "dbl_value")
  ```

# Tests

In order to run the tests, please download the latest quasardb-server and extract in a `qdb` subdirectory like this:

```
mkdir qdb
cd qdb
wget https:////download.quasardb.net/quasardb/2.1/2.1.0-beta.1/server/qdb-2.1.0master-darwin-64bit-server.tar.gz
tar -xzf qdb-2.1.0master-darwin-64bit-server.tar.gz
```

Then launch the integration test using sbt:

```
sbt test
```
# A note for OSX users

QuasarDB uses a C++ standard library that is not shipped with OSX by default. Unfortunately, due to static linking restrictions on OSX this can cause runtime errors such like these:

```
dyld: Symbol not found: __ZTISt18bad_variant_access
```

Until a fix is available for the QuasarDB client libraries, the best course of action is to download the llvm libraries and tell your shell where to find them:

```
cd qdb
wget http://releases.llvm.org/5.0.0/clang+llvm-5.0.0-x86_64-apple-darwin.tar.xz
tar -xzf clang+llvm-5.0.0-x86_64-apple-darwin.tar.xz
```

And then run the tests like this:

```
LD_LIBRARY_PATH=qdb/clang+llvm-5.0.0-x86_64-apple-darwin/lib/ sbt test
```
