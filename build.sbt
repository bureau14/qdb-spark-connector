name := "qdb-spark-connector"
version := "1.0.0-SNAPSHOT"

resolvers += "QuasarDB Maven Repository" at "https://maven.quasardb.net/"

spName := "quasardb/spark-connector"
scalaVersion := "2.11.7"
crossScalaVersions := Seq("2.10.5", "2.11.7")

sparkVersion := "2.2.0"
sparkComponents ++= Seq("streaming", "sql")

organization := "net.quasardb"
organizationName := "QuasarDB"
organizationHomepage := Some(url("https://www.quasardb.net"))

libraryDependencies ++= Seq(
  "net.quasardb" % "qdb" % "2.0.0",
  "org.scalatest" %% "scalatest" % "3.0.4" % "test",
  "com.novocode" % "junit-interface" % "0.9" % "test"
)
