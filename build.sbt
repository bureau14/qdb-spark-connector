name := "qdb-spark-connector"
version := "1.0.0-SNAPSHOT"

spName := "quasardb/spark-connector"
scalaVersion := "2.11.7"
crossScalaVersions := Seq("2.10.5", "2.11.7")

sparkVersion := "1.5.2"
sparkComponents ++= Seq("streaming", "sql")

organization := "com.quasardb"
organizationName := "QuasarDB"
organizationHomepage := Some(url("https://www.quasardb.net"))

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "com.novocode" % "junit-interface" % "0.9" % "test"
)
