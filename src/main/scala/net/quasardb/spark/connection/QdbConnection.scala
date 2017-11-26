package net.quasardb.spark.connection

import net.quasardb.qdb.{QdbSession, QdbCluster};

class QdbConnection extends Serializable {

  Runtime.getRuntime.addShutdownHook(new Thread {
    override def run(): Unit = {
      QdbConnection().close()
    }
  })

  @transient var clusterRef: Option[QdbCluster] = None

  def cluster(uri: String)
    (implicit securityOptions : Option[QdbSession.SecurityOptions]) : QdbCluster = securityOptions match {
    case Some(securityOptions) => new QdbCluster(uri, securityOptions)
    case None => new QdbCluster(uri)
  }

  def close(): Unit = {
    this.synchronized {
      if (clusterRef.isDefined) {
        clusterRef.get.close()
        clusterRef = None
      }
    }
  }
}

object QdbConnection {

  lazy val connection = new QdbConnection()

  def apply() = connection
}
