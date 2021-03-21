package com.miguelhalys.etl

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{desc, explode, lower, split, trim}

object WordCount {

  def apply(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    df.withColumnRenamed("value", "line")
      .withColumn("line", lower($"line"))
      .withColumn("word", explode(split(trim($"line"), " ")))
      .filter($"word" =!= "" || $"word" =!= " ")
      .groupBy($"word")
      .count()
      .orderBy(desc("count"))
  }

}
