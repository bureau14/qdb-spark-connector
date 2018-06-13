scalaVersion := "2.10.6"

resolvers += "Spark Package Main Repo" at "https://dl.bintray.com/spark-packages/maven"
addSbtPlugin("org.spark-packages" % "sbt-spark-package" % "0.2.6")

addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "2.3")
addSbtPlugin("com.jsuereth" % "sbt-pgp" % "1.1.0")
