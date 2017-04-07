name := "ldaTest"

version := "1.0"

scalaVersion := "2.10.4"


libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "2.1.0"
libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "2.1.0"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "2.1.0"
libraryDependencies += "org.apache.spark" % "spark-mllib_2.10" % "2.1.0"
libraryDependencies += "org.apache.spark" % "spark-graphx_2.10" % "2.1.0"
libraryDependencies += "org.apache.spark" % "spark-graphx_2.10" % "2.1.0"
// https://mvnrepository.com/artifact/org.apache.logging.log4j/log4j-core
libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.8.1"
val mysqlConnectorJava = "mysql" % "mysql-connector-java" % "5.1.39"
val sl4jLog4j = "org.slf4j" % "slf4j-log4j12" % "1.7.12"
val typeSafeConfig = "com.typesafe" % "config" % "1.2.1"
val sprayJson = "io.spray" % "spray-json_2.11" % "1.3.2"
val elasticsearch ="org.elasticsearch" % "elasticsearch" % "5.0.2"
val elasticsearchClient= "org.elasticsearch.client" % "transport" % "5.0.2"
libraryDependencies ++= Seq(

  "edu.stanford.nlp" % "stanford-corenlp" % "3.6.0",
  "edu.stanford.nlp" % "stanford-corenlp" % "3.6.0" classifier "models",
  "com.google.protobuf" % "protobuf-java" % "2.6.1",elasticsearch,elasticsearchClient,sprayJson,typeSafeConfig,sl4jLog4j,mysqlConnectorJava
)