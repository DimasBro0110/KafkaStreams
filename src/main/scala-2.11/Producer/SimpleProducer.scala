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
    producerProperties.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384")
    producerProperties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "275600000")
    producerProperties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "2756000")
    producerProperties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "12000")
    producerProperties.put(ProducerConfig.ACKS_CONFIG, "0")
    producerProperties.put(ProducerConfig.LINGER_MS_CONFIG, "10")
    producerProperties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "1500")
    producerProperties
  }

  def sendMessages(amountOfMessages: Long) = {
    println("START SENDING MESSAGES")
    for(i <- 0L to amountOfMessages){
      val messagesBytes = demarshallRecord.generateAvroSingleRecord()
      val producerRecord = new ProducerRecord[String, Array[Byte]](topic, messagesBytes)
//      producer.send(producerRecord, new Callback {
//        override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
//          if(metadata != null) {
//            println("=====INFO=====")
//            println("OFFSET: " + metadata.offset())
//            println("PARTITION: " + metadata.partition())
//            println("=====INFO=====")
//          }else{
//            println(exception)
//          }
//        }
//      })
      producer.send(producerRecord)
    }
  }

}
