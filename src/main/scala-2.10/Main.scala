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

import scala.util.Random

object Main {

  def main(args: Array[String]): Unit = {

    val avroDecoder: AvroInitialization = new AvroInitialization(args(0))
//    val myConsumer = new ConsumerScala(
//      args(1),
//      args(2),
//      args(3),
//      avroDecoder
//    )
//    myConsumer.runConsumer()

    val myProducer = new SimpleProducer(
      args(1),
      args(2),
      avroDecoder
    )

    myProducer.sendMessages(args(3).toLong)


  }

}
