package com.springml.spark.workday.util

import com.springml.spark.workday.model.XPathInput
import org.apache.log4j.Logger
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterEach, FunSuite}

/**
  * Created by sam on 5/10/16.
  */
class CSVUtilTest extends FunSuite with MockitoSugar with BeforeAndAfterEach {
  val logger = Logger.getLogger(classOf[CSVUtilTest])

  test("Successfully reading CSV file") {
    val csvURL= getClass.getResource("/namespaces.csv")
    val csvContent = CSVUtil.readCSV(csvURL.getPath)
    assert(csvContent.size == 1)
    logger.error(csvContent)
    assert(csvContent("wd").equals("urn:com.workday/bsvc"))
  }

  test("Successfully populate XpathInput") {
    val csvURL= getClass.getResource("/xpath.csv")
    val objectTag = "//wd:Customer_Invoice"
    val detailsTag = "/wd:Customer_Invoice/wd:Customer_Invoice_Data/wd:Customer_Invoice_Line_Replacement_Data"
    val xpathInput = new XPathInput(objectTag, detailsTag)

    CSVUtil.populateXPathInput(csvURL.getPath, xpathInput)
    assert(xpathInput.detailsMap.size == 2)
    assert(xpathInput.detailsMap("Analytical_Amount").equals("//wd:Analytical_Amount/text()"))

    assert(xpathInput.headersMap.size == 1)
    assert(xpathInput.headersMap("InvoiceId").equals("/wd:Customer_Invoice/wd:Customer_Invoice_Data/wd:Customer_Invoice_ID/text()"))
  }
}
