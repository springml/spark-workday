package com.springml.spark.workday

import com.springml.spark.workday.model.XPathInput
import com.springml.spark.workday.util.{CSVUtil, XPathHelper}
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.scalatest.mock.MockitoSugar

import scala.xml.XML

/**
  * Created by sam on 6/10/16.
  */
class WWSReaderTest extends FunSuite with MockitoSugar with BeforeAndAfterEach {
  test ("Test Modify Page") {
    val requestXMLStream = getClass().getResourceAsStream("/Get_Customer_Invoices_Request_Sample.xml")
    val xmlContent = scala.io.Source.fromInputStream(requestXMLStream).mkString

    val dummyXPathInput = new XPathInput(null, null)
    val wWSReader = new WWSReader(null, dummyXPathInput)
    wWSReader.currentPage = 10

    val modifiedContent = wWSReader.withModifiedPage(xmlContent)
    println(modifiedContent)
    val modXML = XML.loadString(modifiedContent)
    val page = (modXML \\ "Response_Filter" \ "Page").text.toLong

    assert(wWSReader.currentPage == page)
  }
}
