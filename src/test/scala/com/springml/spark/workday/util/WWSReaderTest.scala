package com.springml.spark.workday

import com.springml.spark.workday.model.XPathInput
import com.springml.spark.workday.util.CSVUtil
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterEach, FunSuite}

import scala.collection.mutable.ListBuffer
import scala.xml.XML

/**
  * Created by sam on 6/10/16.
  */
class WWSReaderTest extends FunSuite with MockitoSugar with BeforeAndAfterEach {
  test ("Test Modify Page") {
    val xmlContent = getResourceContent("/Get_Customer_Invoices_Request_Sample.xml")

    val dummyXPathInput = new XPathInput(null, null)
    val wWSReader = new WWSReader(null, dummyXPathInput)
    wWSReader.currentPage = 10

    val modifiedContent = wWSReader.withModifiedPage(xmlContent)
    println(modifiedContent)
    val modXML = XML.loadString(modifiedContent)
    val page = (modXML \\ "Response_Filter" \ "Page").text.toLong

    assert(wWSReader.currentPage == page)
  }

  test ("Test WWSReader read") {
    val csvURL = getClass.getResource("/xpath.csv")
    val objectTag = "//wd:Customer_Invoice"
    val detailsTag = "/wd:Customer_Invoice/wd:Customer_Invoice_Data/wd:Customer_Invoice_Line_Replacement_Data"
    val xpathInput = new XPathInput(objectTag, detailsTag)

    val namespaceFile = getClass.getResource("/namespaces.csv").getPath
    xpathInput.namespaceMap = CSVUtil.readCSV(namespaceFile)

    CSVUtil.populateXPathInput(csvURL.getPath, xpathInput)

    var resultListBuf = new ListBuffer[String]()
    resultListBuf += getResourceContent("/Customer_Invoice_1.xml")
    resultListBuf += getResourceContent("/Customer_Invoice_2.xml")

    val wWSReader = new WWSReader(null, xpathInput)
    val results = wWSReader.read(resultListBuf.toList)

    assert(results.size == 2)
    val firstRecord = results(0)

    assert(firstRecord("Quantity").equals("1000.00"))
  }

  private def getResourceContent(path: String): String = {
    val resStream = getClass().getResourceAsStream(path)
    scala.io.Source.fromInputStream(resStream).mkString
  }

}
