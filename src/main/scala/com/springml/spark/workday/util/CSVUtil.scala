package com.springml.spark.workday.util

import java.util

/**
  * Created by sam on 20/9/16.
  */
object CSVUtil {
  def readCSV(csvLocation : String) : Map[String, String] = {
    var resultMap : Map[String, String] = Map.empty

    if (csvLocation != null) {
      val bufferedSource = scala.io.Source.fromFile(csvLocation)

      for (line <- bufferedSource.getLines) {
        val cols = line.split(",").map(_.trim)
        resultMap += cols(0) -> cols(1)
      }
      bufferedSource.close()
    }

    resultMap
  }
}
