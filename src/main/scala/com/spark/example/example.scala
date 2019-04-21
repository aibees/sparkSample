package com.spark.example
import java.io.File

import com.spark.helper._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object example {
  //val configFile = new File("com/apach/hadoop/config/aibees.conf")
  //val value = ConfigFactory.parseFile(configFile).getConfig("hadoop").toString

  val value = "log/*"
  def main(args: Array[String]): Unit = {
    val sessionMaker = new sparkHelper
    val sc: SparkSession = sessionMaker.createSession(this.getClass.getName)

    //read text from hdfs
    val text: DataFrame = sc.read.textFile(value).toDF()
    // split df to 10 columns
    val splitedText = text.withColumn("tmp", split(col("value"), "\t"))
      .select(
        col("tmp").getItem(0).as("query"),
        col("tmp").getItem(1).as("dateTime"),
        col("tmp").getItem(2).as("cookie"),
        col("tmp").getItem(3).as("cuve"),
        col("tmp").getItem(4).as("sessionId"),
        col("tmp").getItem(5).as("area"),
        col("tmp").getItem(6).as("rank"),
        col("tmp").getItem(7).as("signature"),
        col("tmp").getItem(8).as("gdid"),
        col("tmp").getItem(9).as("url")
      )
    splitedText.printSchema()
    println("show")
    splitedText.show()
    //println(value)
  }
}