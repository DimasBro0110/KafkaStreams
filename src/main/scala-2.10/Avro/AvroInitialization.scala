package Avro

/**
  * Created by Dmitry on 19.04.2017.
  */

import java.io.{ByteArrayOutputStream, File}

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.io.{BinaryEncoder, DatumReader, DecoderFactory, EncoderFactory}
import org.apache.avro.specific.{SpecificDatumReader, SpecificDatumWriter}


class AvroInitialization(
                          val filePath: String
                        ) {

  private val avroSchema: Schema = getSchema

  private def getSchema: Schema = {
    val fileAvro: File = new File(filePath)
    new Schema.Parser().parse(fileAvro)
  }

  def decodeByteFlow(incomeRecord: Array[Byte]): GenericRecord = {
    try {
      println("SCHEMA INITIALIZED SUCCESSFULLY")
      val byteReader: DatumReader[GenericRecord] = new SpecificDatumReader[GenericRecord](avroSchema)
      val binaryDecoder = DecoderFactory.get().binaryDecoder(incomeRecord, null)
      val decodedData: GenericRecord = byteReader.read(null, binaryDecoder)
      decodedData
    }catch {
      case ex: Exception => ex.printStackTrace()
      null
    }
  }

  def generateAvroRandomRecords(amountOfRecords: Long): Array[Byte] = {
    val binaryWriter = new SpecificDatumWriter[GenericRecord](avroSchema)
    val out = new ByteArrayOutputStream()
    val binaryEncoder: BinaryEncoder = EncoderFactory.get().binaryEncoder(out, null)
    for(i <- 0L to amountOfRecords){
      val tempRecord: GenericRecord = new GenericData.Record(avroSchema)
      tempRecord.put("imsi", "imsi_test")
      tempRecord.put("msisdn", "msisdn_test")
      tempRecord.put("imei", "imei_test")
      tempRecord.put("lac", "1488".toLong)
      tempRecord.put("cellid", "1488".toLong)
      tempRecord.put("mcc", "1488".toInt)
      tempRecord.put("mnc", "1488".toInt)
      tempRecord.put("locationType", "1488".toInt)
      tempRecord.put("startTime", "1488".toLong)
      tempRecord.put("endTime", "1488".toLong)
      tempRecord.put("aggregationEventCount", "1488".toLong)
      tempRecord.put("targetIp", "imsi_test")
      tempRecord.put("targetPort", "1488".toInt)
      tempRecord.put("cookies", "imsi_test")
      tempRecord.put("host", "imsi_test")
      tempRecord.put("path", "imsi_test")
      tempRecord.put("downloadKb", "1488".toLong)
      tempRecord.put("uploadKb", "1488".toLong)
      binaryWriter.write(tempRecord, binaryEncoder)
    }
    binaryEncoder.flush()
    out.close()
    out.toByteArray
  }

}
