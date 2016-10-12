package com.springml.spark.workday.util

import com.springml.spark.workday.model.XPathInput
import org.apache.log4j.Logger

/**
  * Created by sam on 20/9/16.
  */
object CSVUtil {
  @transient val logger = Logger.getLogger(this.getClass.getName)

  def readCSV(csvLocation : String) : Map[String, String] = {
    var resultMap : Map[String, String] = Map.empty

    if (csvLocation != null) {
      val bufferedSource = scala.io.Source.fromFile(csvLocation)

      for (line <- bufferedSource.getLines) {
        if (!line.startsWith("#")) {
          val cols = line.split(",").map(_.trim)
          if (cols.length != 2) {
            throw new Exception("Invalid Row : " + line + "\n Please make sure rows are specified as 'Prefix','namespace' in " + csvLocation)
          }

          resultMap += cols(0) -> cols(1)
        } else {
          logger.info("Ignoring commented line " + line)
        }
      }
      bufferedSource.close()
    }

    resultMap
  }

  private def details(xpath: String, detailTag : String): String = {
    var modXpath = xpath
    var modDetailTag = detailTag

    val colIndex = detailTag.indexOf(":")
    if (colIndex != -1) {
      modDetailTag = detailTag.substring(colIndex + 1)
    }

    val detailTagIndex = xpath.indexOf(modDetailTag)
    if (detailTagIndex != -1) {
      modXpath = "/" + xpath.substring(detailTagIndex + modDetailTag.length)
    }

    logger.debug("Modified XPath : " + modXpath)
    modXpath
  }

  def populateXPathInput(csvLocation : String, xPathInput: XPathInput) = {
    if (csvLocation != null) {
      val bufferedSource = scala.io.Source.fromFile(csvLocation)

      for (line <- bufferedSource.getLines) {
        if (!line.startsWith("#")) {
          val cols = line.split(",").map(_.trim)
          if (cols.length != 3) {
            throw new Exception("Invalid Row : " + line +
              "\n Please make sure rows are specified as 'Type','FieldName','XPath' in " + csvLocation)
          }

          val elementType = cols(0)
          val fieldName = cols(1)
          val xpath = cols(2)

          if ("details".equalsIgnoreCase(elementType)) {
            xPathInput.detailsMap += fieldName -> details(xpath, xPathInput.detailTag)
          } else if ("headers".equalsIgnoreCase(elementType)) {
            xPathInput.headersMap += fieldName -> xpath
          } else {
            throw new Exception("Invalid Type : '" + elementType + "' in " + csvLocation +
              ". Supported types are 'headers' and 'details'")
          }
        } else {
          logger.info("Ignoring commented line " + line)
        }
      }

      bufferedSource.close()
    }
  }

}
