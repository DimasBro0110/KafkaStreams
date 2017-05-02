package Consumer

import java.util.{Collections, Properties}

import Avro.AvroInitialization
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}

import scala.collection.JavaConversions._

/**
  * Created by Dmitry on 18.04.2017.
  */
class ConsumerScala(val brokers: String,
                    val groupId: String,
                    val topic: String,
                    val demarshallRecord: AvroInitialization) {

  private val recordDecoder: AvroInitialization = demarshallRecord
  private val props: Properties = buildConsumerConfig(brokers, groupId)
  private val consumer = new KafkaConsumer[String, Array[Byte]](props)
  private var flagRun: Boolean = true

  private def buildConsumerConfig(brokers: String, groupId: String): Properties = {
    val consumerProperties = new Properties()
    consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    consumerProperties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
    consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    println(recordDecoder != null)
    println(consumerProperties)
    consumerProperties
  }

  def setFlag(flag: Boolean) = {
    this.flagRun = flag
  }

  def runConsumer() = {
    consumer.subscribe(Collections.singletonList(topic))
    println("runConsumer started!!!")
    while(flagRun){
      val inputRecords = consumer.poll(100)
      for(curRecord <- inputRecords){
        println("MESSAGE: ")
        val curKey: String = curRecord.key()
        val genericRecordFromIncomingStream: GenericRecord = recordDecoder.decodeByteFlow(curRecord.value())
        if(genericRecordFromIncomingStream != null) {
          println("Key: " + curKey + "\nValue: " + genericRecordFromIncomingStream.toString)
        }else{
          println("CAN\'T DEMARSHALL !!!")
        }
      }
    }
  }

}
