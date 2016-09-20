package com.springml.spark.workday.util

import java.util

/**
  * Created by sam on 20/9/16.
  */
object CSVUtil {
  def readCSV(csvLocation : String) : util.HashMap[String, String] = {
    val resultMap = new util.HashMap[String, String]

    if (csvLocation != null) {
      val bufferedSource = scala.io.Source.fromFile(csvLocation)

      for (line <- bufferedSource.getLines) {
        val cols = line.split(",").map(_.trim)
        resultMap.put(cols(0), cols(1))
      }
      bufferedSource.close()
    }

    resultMap
  }
}
