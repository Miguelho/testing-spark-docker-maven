package com.miguelhalys

import org.apache.spark.sql.SparkSession

trait SparkProvider {

  implicit lazy val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Etl")
      .master("local[*]")
      .getOrCreate()

}
