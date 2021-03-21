package com.miguelhalys

import com.miguelhalys.etl._
import com.miguelhalys.io.{Input, Output}

import scala.util.{Failure, Success, Try}

object Main extends SparkProvider {

  def main(args: Array[String]): Unit = {

    spark.sparkContext.setLogLevel("error")

    val config = args match {
      case Array(input, output) => Map("input" -> input,"output" -> output)
      case Array(input) => Map("input" -> input)
    }

    val inputPath = config("input")
    val outputPath = config("output")

    val inputDf = Input.read(inputPath).get

    val df = WordCount(inputDf)

    Output.write(df, outputPath) match {
      case Success(_) => println("Job Completed")
      case Failure(exception) => println(s"An exception occurred $exception" )
    }

    spark.close()
  }

}
