package Producer

import java.util.Properties

import Avro.AvroInitialization
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig, ProducerRecord}

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
    producerProperties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "67108864")
    producerProperties
  }

  def sendMessages(amountOfMessages: Long) = {
    println("START SENDING MESSAGES")
    val messagesBytes = demarshallRecord.generateAvroRandomRecords(amountOfMessages)
    val producerRecord = new ProducerRecord[String, Array[Byte]](topic, messagesBytes)
    producer.send(producerRecord)
  }

}
