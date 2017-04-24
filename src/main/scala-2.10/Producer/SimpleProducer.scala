package Producer

import java.util.Properties

import Avro.AvroInitialization
import org.apache.kafka.clients.producer._

/**
  * Created by Dmitry on 19.04.2017.
  */
class SimpleProducer(val brokers: String,
                     val topic: String,
                     val demarshallRecord: AvroInitialization) {

  private val producerProperties: Properties = buildProducerConfig(brokers)
  private val producer = new KafkaProducer[String, Array[Byte]](producerProperties)

  private def buildProducerConfig(brokers: String): Properties = {
    val producerProperties: Properties = new Properties()
    producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.ByteArraySerializer")
    producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.ByteArraySerializer")
    producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    producerProperties.put(ProducerConfig.BATCH_SIZE_CONFIG, "65536")
    producerProperties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "67108864")
    producerProperties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "470000064")
    producerProperties.put(ProducerConfig.ACKS_CONFIG, "1")
    producerProperties
  }

  def sendMessages(amountOfMessages: Long) = {
    println("START SENDING MESSAGES")
    for(i <- 0L to amountOfMessages){
      val messagesBytes = demarshallRecord.generateAvroSingleRecord()
      val producerRecord = new ProducerRecord[String, Array[Byte]](topic, messagesBytes)
      producer.send(producerRecord, new Callback {
        override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
          if(metadata != null) {
            println("=====INFO=====")
            println("OFFSET: " + metadata.topic())
            println("PARTITION: " + metadata.partition())
            println("=====INFO=====")
          }else{
            println(exception)
          }
        }
      })
    }
  }

}
