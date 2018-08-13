package com.seigneurin

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

object Producer {
  def main(args: Array[String]): Unit = {

    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", classOf[StringSerializer])
    props.put("value.serializer", classOf[StringSerializer])

    val producer = new KafkaProducer[String, String](props)

    val fakePersonJson = """{"firstName":"Gilbert","lastName":"Crist","birthDate":"1957-08-01T14:37:33.727+0000"}"""
    while (true) {
      val futureResult = producer.send(new ProducerRecord("persons", fakePersonJson))

      Thread.sleep(1000)

      // wait for the write acknowledgment
      futureResult.get()
    }
  }
}
