import Avro.AvroInitialization
import Consumer.ConsumerScala
import ProducerPool.{ProducerPool, ProducerThread}

object Main {

  def main(args: Array[String]): Unit = {

    val executionMode: String = args(0)
    if(executionMode.equals("consumer")) {
      val avroDecoder: AvroInitialization = new AvroInitialization(args(1))
          val myConsumer = new ConsumerScala(
            args(2),
            args(3),
            args(4),
            avroDecoder
          )
          myConsumer.runConsumer()
    }else if(executionMode.equals("producer")) {
      val avroDecoder: AvroInitialization = new AvroInitialization(args(1))
      //      val myProducer = new SimpleProducer(
      //        args(2),
      //        args(3),
      //        avroDecoder
      //      )
      //      myProducer.sendMessages(args(4).toLong)
      //    }
      val producerInstance: ProducerThread = new ProducerThread(
        args(2),
        args(3),
        avroDecoder
      )
      val poolOfProducers: ProducerPool = new ProducerPool(3, producerInstance)
      poolOfProducers.run()
    }else if(executionMode.equals("streams")){
      val brokers: String = args(1)
      val topicReadFrom: String = args(2)
      val topicSaveTo: String = args(3)
      val streamInstance = new JavaKafkaStreams(brokers, topicReadFrom, topicSaveTo)
      streamInstance.runStream()
    }


  }
}
