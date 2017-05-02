name := "KafkaStreams"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(

  "org.apache.kafka" % "kafka-streams" % "0.10.0.0",
  "org.apache.kafka" % "kafka-clients" % "0.10.0.0",
  "org.apache.avro"  %  "avro"  %  "1.7.7",
  "com.google.code.gson" % "gson" % "2.5"

)

assemblyJarName in assembly := "kafka-streams.jar"