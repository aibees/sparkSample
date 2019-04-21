package com.spark.helper

import org.apache.spark.sql.SparkSession

class sparkHelper {
  def createSession(appName: String): SparkSession = {
    val spark : SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName(appName)
      .getOrCreate()
    spark
  }
}
