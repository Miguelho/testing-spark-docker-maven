package com.miguelhalys.io

import org.apache.spark.sql.DataFrame

import scala.util.Try

trait Output {

  def write(df: DataFrame, path: String): Try[Unit]

}

object Output extends Output {

  override def write(df: DataFrame, path: String): Try[Unit] = {
    Try {
      df.coalesce(1).write.mode("overwrite").parquet(path)
    }
  }

}

