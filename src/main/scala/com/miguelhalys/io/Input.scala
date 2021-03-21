package com.miguelhalys.io

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try

trait Input {

  def read(path: String)(implicit spark: SparkSession): Try[DataFrame]

}

object Input extends Input {

  override def read(path: String)(implicit spark: SparkSession): Try[DataFrame] =
    Try {
      spark.read.text(path)
    }

}
