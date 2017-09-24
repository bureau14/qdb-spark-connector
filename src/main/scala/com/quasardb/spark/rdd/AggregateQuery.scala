package com.quasardb.spark.rdd

import java.sql.Timestamp

import net.quasardb.qdb._

case class AggregateQuery(
  begin: Timestamp,
  end: Timestamp,
  operation: QdbAggregation.Type
) extends Serializable


// :TODO: it would probably be nice if this separation between query
// and result would also be part of the java API. Or that we would have
// a factory where we could instantiate the java type here without knowing
// whether it's a Double or a Blob or whatever.
