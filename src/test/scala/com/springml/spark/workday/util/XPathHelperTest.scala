package com.springml.spark.workday.util

import com.springml.spark.workday.model.XPathInput
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterEach, FunSuite}

/**
  * Created by sam on 5/10/16.
  */
class XPathHelperTest extends FunSuite with MockitoSugar with BeforeAndAfterEach {
  test ("Test evaluate") {
    val csvURL = getClass.getResource("/xpath.csv")
    val objectTag = "//wd:Customer_Invoice"
    val detailsTag = "/wd:Customer_Invoice/wd:Customer_Invoice_Data/wd:Customer_Invoice_Line_Replacement_Data"
    val xpathInput = new XPathInput(objectTag, detailsTag)

    CSVUtil.populateXPathInput(csvURL.getPath, xpathInput)

    val namespaceFile = getClass.getResource("/namespaces.csv").getPath
    val helper = new XPathHelper(CSVUtil.readCSV(namespaceFile), null)

    val responseXMLStream = getClass().getResourceAsStream("/Get_Customer_Invoices_Response_Sample.xml")
    val xmlContent = scala.io.Source.fromInputStream(responseXMLStream).mkString
    val evaluateResult = helper.evaluate(objectTag, xmlContent)
    assert(evaluateResult != null)
    assert(evaluateResult.size == 2)
  }

  test ("Test evaluate as String") {
    val csvURL = getClass.getResource("/xpath.csv")
    val objectTag = "//wd:Customer_Invoice"
    val detailsTag = "/wd:Customer_Invoice/wd:Customer_Invoice_Data/wd:Customer_Invoice_Line_Replacement_Data"
    val xpathInput = new XPathInput(objectTag, detailsTag)

    CSVUtil.populateXPathInput(csvURL.getPath, xpathInput)

    val namespaceFile = getClass.getResource("/namespaces.csv").getPath
    val helper = new XPathHelper(CSVUtil.readCSV(namespaceFile), null)

    val responseXMLStream = getClass().getResourceAsStream("/Get_Customer_Invoices_Response_Sample.xml")
    val xmlContent = scala.io.Source.fromInputStream(responseXMLStream).mkString
    val evaluateResult = helper.evaluate(objectTag, xmlContent)
    assert(evaluateResult != null)
    assert(evaluateResult.size == 2)

    val invoiceIdXPath = "/wd:Customer_Invoice/wd:Customer_Invoice_Data/wd:Customer_Invoice_ID/text()";
    val invoiceId = helper.evaluateToString(invoiceIdXPath, evaluateResult(0))
    assert(invoiceId != null)
    assert(invoiceId.equals("1234"))
  }
}
