name := "BatchLayer"

version := "0.1"

scalaVersion := "2.12.10"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.5.0"

libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "3.3.0"
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "3.3.0"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "3.3.0"
libraryDependencies += "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "3.3.0"

libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"