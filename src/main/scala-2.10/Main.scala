/**
  * Created by Dmitry on 14.04.2017.
  */

import Avro.AvroInitialization
import Consumer.ConsumerScala
import Producer.SimpleProducer
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.kstream.{KStream, KStreamBuilder}
import org.apache.kafka.clients.producer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object Main {

  def main(args: Array[String]): Unit = {

    val avroDecoder: AvroInitialization = new AvroInitialization(args(0))
    val myConsumer = new ConsumerScala(
      args(1),
      //"192.168.6.23:9092,192.168.6.25:9092,192.168.6.24:9092",
      //"consumer_group",
      //"tele2_sample_1P"
      //
      args(2),
      args(3),
      avroDecoder
    )
    myConsumer.runConsumer()

//    val myProducer = new SimpleProducer(
//      args(1),
//      args(2),
//      avroDecoder
//    )
//
//    myProducer.sendMessages(args(3).toLong)

  }

}
