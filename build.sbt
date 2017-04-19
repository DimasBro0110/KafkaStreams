name := "KafkaStreams"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies ++= Seq(

  "org.apache.spark" % "spark-core_2.10" % "1.6.1" % "provided",
  "org.apache.spark" % "spark-sql_2.10" % "1.6.1" % "provided",
  "com.databricks" % "spark-csv_2.10" % "1.5.0",
  "org.apache.kafka" % "kafka-streams" % "0.10.0.0",
  "org.apache.kafka" % "kafka-clients" % "0.10.0.0",
  "org.apache.avro"  %  "avro"  %  "1.7.7"


)

assemblyJarName in assembly := "kafka-streams.jar"