package com.springml.spark.workday.util

import com.springml.spark.workday.model.XPathInput

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

  def populateXPathInput(csvLocation : String, xPathInput: XPathInput) = {
    if (csvLocation != null) {
      val bufferedSource = scala.io.Source.fromFile(csvLocation)

      for (line <- bufferedSource.getLines) {
        val cols = line.split(",").map(_.trim)
        val elementType = cols(0)
        val fieldName = cols(1)
        val xpath = cols(2)

        if ("details".equalsIgnoreCase(elementType)) {
          xPathInput.detailsMap += fieldName -> xpath
        } else {
          xPathInput.headersMap += fieldName -> xpath
        }
      }

      bufferedSource.close()
    }
  }

}
