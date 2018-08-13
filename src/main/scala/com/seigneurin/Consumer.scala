package com.seigneurin

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDate, Period, ZoneId}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructType}

object Consumer {
  val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ")

  def main(args: Array[String]): Unit = {
    new Consumer("localhost:9092").process()
  }
}

class Consumer(brokers: String) {

  def process(): Unit = {

    val spark = SparkSession.builder()
      .appName("kafka-tutorials")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val personJsonDf = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("subscribe", "persons")
      .load()
      .selectExpr("CAST(value AS STRING)")

    val struct = new StructType()
      .add("firstName", DataTypes.StringType)
      .add("lastName", DataTypes.StringType)
      .add("birthDate", DataTypes.StringType)

    val personDf = personJsonDf.select(from_json($"value", struct).as("person"))
      .select($"person.firstName", $"person.lastName", $"person.birthDate")

    val u1: String => Int = s => {
      val birthDate = Consumer.dateTimeFormatter.parse(s)
      val birthDateLocal = Instant.from(birthDate).atZone(ZoneId.systemDefault()).toLocalDate()
      val age = Period.between(birthDateLocal, LocalDate.now()).getYears()
      age
    }
    val u: UserDefinedFunction = udf(u1, DataTypes.IntegerType)
    val processedDf = personDf.withColumn("age", u($"birthDate"))

    val consoleOutput = processedDf.writeStream
      .outputMode("append")
      .format("console")
      .start()

    consoleOutput.awaitTermination()
  }
}
