package com.springml.spark.workday

import com.springml.spark.workday.model.{WWSInput, XPathInput}
import com.springml.spark.workday.util.{XPathHelper, XercesWarningFilter}
import com.springml.spark.workday.ws.WWSClient
import com.springml.spark.workday.xml.PageElementRewriteRule
import org.apache.log4j.Logger

import scala.collection.mutable
import scala.xml.transform.{RewriteRule, RuleTransformer}
import scala.xml.{Elem, Node, XML}

/**
  * Created by sam on 28/9/16.
  */
class WWSReader(
               val wwsInput: WWSInput,
               val xPathInput: XPathInput
               ) {

  @transient val logger = Logger.getLogger(classOf[WWSReader])

  var currentPage = 0l
  var totalPages = 0l
  val xPathHelper = new XPathHelper(xPathInput.namespaceMap, null)

  def read() : List[mutable.Map[String, String]] = {
    XercesWarningFilter.start()
    var records :List[mutable.Map[String, String]] = List.empty

    var response : String = ""
    do {
      wwsInput.request = withModifiedPage(wwsInput.request)
      response = new WWSClient(wwsInput) execute()
      logger.debug("Response from WWS " + response)

      if (response != null && !response.isEmpty) {
        val xmlRecords = xPathHelper.evaluate(xPathInput.objectTag, response)

        records ++= read(xmlRecords)
      }
    }
    while (moreToRead(response))

    records
  }

  def withModifiedPage(request: String) : String = {
    currentPage += 1
    logger.info("Accessing WWS page " + currentPage)

    val perr = new PageElementRewriteRule(currentPage.toString)
    object rt extends RuleTransformer(perr)

    object ResponseFilterRewriteRule extends RewriteRule {
      override def transform(n: Node): Seq[Node] = n match {
        case sn @ Elem(_, "Response_Filter", _, _, _*) => rt(sn)
        case other => other
      }
    }

    object pageTransformer extends RuleTransformer(ResponseFilterRewriteRule)

    val xmlRequest = XML.loadString(request)
    val modXML = pageTransformer(xmlRequest)

    modXML.buildString(false)
  }

  private def moreToRead(wwsResponse : String) : Boolean = {
    if (wwsResponse == null || wwsResponse.isEmpty) {
      return false
    }

    // To avoid unnecessary parsing
    if (totalPages == 0l) {
      // Reading total pages and comparing it with currentPage
      val responseXML = XML.loadString(wwsResponse)
      totalPages = (responseXML \\ "Response_Results" \ "Total_Pages").text.toLong
      logger.info("Total pages : " + totalPages)
    }

    logger.debug("Total Pages : " + totalPages)
    logger.debug("Current Page : " + currentPage)
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
      logger.debug("Xpath evaluation response for xpath " + xpath + " \n" + result)
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

    logger.debug("XPath Details Tag : " + xPathInput.detailTag)
    logger.debug("Row : " + row)
    val details = xPathHelper.evaluate(xPathInput.detailTag, row)
    logger.debug("Details Count " + details.size)

    details.map(detail => read(detail, headerRecord))
  }

}
