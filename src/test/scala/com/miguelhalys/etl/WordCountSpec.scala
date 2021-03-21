package com.miguelhalys.etl

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.scalatest.{Matchers, WordSpec}

class WordCountSpec extends WordSpec with Matchers with DataFrameSuiteBase {

  implicit lazy val iSpark: SparkSession = spark

  "WordCount" should {
    "Count words" in {

      val schema = StructType(Seq(StructField("line", StringType)))
      val rdd = spark.sparkContext.parallelize(Seq(Row("An antilope is an animal")))
      val input = spark.createDataFrame(rdd, schema)

      val schema2 = StructType(Seq(
        StructField("word", StringType),
        StructField("count", LongType)))
      val rdd2 = spark.sparkContext.parallelize(Seq(
        Row("An", 2L),
        Row("antilope", 1L),
        Row("is", 1L),
        Row("animal", 1L)))

      val expected = spark.createDataFrame(rdd2, schema2)

      assertDataFrameDataEquals(expected, WordCount(input))
    }
  }

}
