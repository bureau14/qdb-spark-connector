val qdbVersion = Option(System.getProperty("qdb.api.version")) match {
  case Some(version) => version
  case None => "0.0.1-SNAPSHOT"
}

name := "qdb-spark-connector"
version := qdbVersion

resolvers += Resolver.mavenLocal
spName := "quasardb/spark-connector"
scalaVersion := "2.10.6"
crossScalaVersions := Seq("2.10.5", "2.11.7")

sparkVersion := "2.2.0"
sparkComponents ++= Seq("streaming", "sql")

organization := "net.quasardb"
organizationName := "QuasarDB"
organizationHomepage := Some(url("https://www.quasardb.net"))

libraryDependencies ++= Seq(
  "me.lessis" %% "retry" % "0.2.0",
  "org.scalatest" %% "scalatest" % "3.0.4" % "test",
  "com.novocode" % "junit-interface" % "0.9" % "test",

  "net.quasardb" % "qdb" % qdbVersion,
  "net.quasardb" % "jni" % qdbVersion,
  "net.quasardb" % "jni" % qdbVersion classifier "linux-x86_64",
  "net.quasardb" % "jni" % qdbVersion classifier "osx-x86_64",
  "net.quasardb" % "jni" % qdbVersion classifier "freebsd-x86_64",
  "net.quasardb" % "jni" % qdbVersion classifier "windows-x86_64",
  "net.quasardb" % "jni" % qdbVersion classifier "windows-x86_32"
)

parallelExecution in Test := false

scalacOptions += "-feature"
scalacOptions += "-deprecation"
