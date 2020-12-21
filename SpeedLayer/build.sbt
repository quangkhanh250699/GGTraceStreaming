name := "SpeechLayer"

version := "0.1"

scalaVersion := "2.12.10"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka-0-10
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.0.1"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.0.1"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.0.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.1"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.0.1"
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.1"

// assemblyMergeStrategy in assembly := {
// 
//   case x if x.startsWith("META-INF") => MergeStrategy.discard // Bumf
//   case x if x.endsWith(".html") => MergeStrategy.discard // More bumf
//   case x if x.contains("slf4j-api") => MergeStrategy.last
//   case x if x.contains("org/cyberneko/html") => MergeStrategy.first
//   case PathList("com", "esotericsoftware", xs@_ *) => MergeStrategy.last // For Log$Logger.class
//   case "git.properties" => MergeStrategy.discard
//   case "javax/inject/Inject.class" => MergeStrategy.discard
//   case "javax/inject/Named.class" => MergeStrategy.discard
//   case _ => MergeStrategy.last
//  case x =>
//    val oldStrategy = (assemblyMergeStrategy in assembly).value
//    oldStrategy(x)
// 
// }
