package com.springml.spark.workday

import com.springml.spark.workday.model.{WWSInput, XPathInput}
import com.springml.spark.workday.util.XPathHelper
import com.springml.spark.workday.ws.WWSClient
import com.springml.spark.workday.xml.{ElementTransformer, ParentNodeTransformer}
import org.apache.log4j.Logger

import scala.collection.mutable
import scala.xml.XML
import scala.xml.transform.RuleTransformer

/**
  * Created by sam on 28/9/16.
  */
class WWSReader(
               val wwsInput: WWSInput,
               val xPathInput: XPathInput
               ) {

  @transient val logger = Logger.getLogger(classOf[DefaultSource])

  var currentPage = 0l
  var totalPages = 0l
  val xPathHelper = new XPathHelper(xPathInput.namespaceMap, null)

  def read() : List[mutable.Map[String, String]] = {
    var records :List[mutable.Map[String, String]] = List.empty

    var response : String = ""
    wwsInput.request = withModifiedCount(wwsInput.request)
    do {
      wwsInput.request = withModifiedPage(wwsInput.request)
      response = new WWSClient(wwsInput) execute()
      logger.debug("Response from WWS " + response)

      val xmlRecords = xPathHelper.evaluate(xPathInput.objectTag, response)

      records ++= read(xmlRecords)
    }
    while (moreToRead(response))

    records
  }

  private def withModifiedPage(request: String) : String = {
    currentPage += 1
    withModifiedContent(request, "Response_Filter", "Page" , currentPage.toString)
  }

  private def withModifiedCount(request: String) : String = {
    withModifiedContent(request, "Response_Filter", "Count" , "100")
  }

  private def withModifiedContent(request : String, parentNode : String,
                                  elementName : String, elementValue : String) : String = {
    val elementTransformer = new ElementTransformer(elementName, elementValue)
    object et extends RuleTransformer(elementTransformer)

    val parentNodeTransformer = new ParentNodeTransformer(parentNode, et)
    object pnt extends RuleTransformer(parentNodeTransformer)

    val xmlRequest = XML.loadString(request)
    val modXml = pnt(xmlRequest)

    modXml.buildString(false)
  }

  private def moreToRead(wwsResponse : String) : Boolean = {
    // To avoid unnecessary parsing
    if (totalPages == 0l) {
      // Reading total pages and comparing it with currentPage
      val responseXML = XML.loadString(wwsResponse)
      totalPages = (responseXML \\ "Response_Results" \ "Page_Results").text.toLong
    }

    logger.info("Total Pages : " + totalPages)
    logger.info("Current Page : " + currentPage)
    totalPages > currentPage
  }

  private def read(xmlRecords : List[String]) : List[scala.collection.mutable.Map[String, String]] = {
    val recordLists = xmlRecords.map(row => read(row))
    var records : List[mutable.Map[String, String]] = List.empty

    for (recordList <- recordLists) {
      records ++= recordList
    }

    records
  }

  private def read(detail: String, headerRecord: mutable.Map[String, String]): mutable.Map[String, String] = {
    var record = scala.collection.mutable.Map[String, String]()
    record ++= headerRecord
    for ((column, xpath) <- xPathInput.detailsMap) {
      val result = xPathHelper.evaluateToString(xpath, detail)
      logger.info("Xpath evaluation response for xpath " + xpath + " \n" + result)
      record(column) = result
    }

    record
  }

  private def read(row: String): List[scala.collection.mutable.Map[String, String]] = {
    val headerRecord = scala.collection.mutable.Map[String, String]()
    for ((column, xpath) <- xPathInput.headersMap) {
      val result = xPathHelper.evaluateToString(xpath, row)
      logger.debug("Xpath evaluation response for xpath " + xpath + " \n" + result)
      headerRecord(column) = result
    }

    val details = xPathHelper.evaluate(xPathInput.detailTag, row)
    logger.info("Details Count " + details.size)

    details.map(detail => read(detail, headerRecord))
  }

}
