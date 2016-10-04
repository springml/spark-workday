package com.springml.spark.workday.ws

import java.io.{StringReader, StringWriter}
import javax.xml.transform.stream.{StreamResult, StreamSource}

import com.springml.spark.workday.model.WWSInput
import org.apache.log4j.Logger
import org.springframework.ws.client.core.WebServiceTemplate

/**
  * Created by sam on 20/9/16.
  */
class WWSClient(
               val wwsInput : WWSInput
               ) {

  @transient val logger = Logger.getLogger(classOf[WWSClient])

  private val webServiceTemplate = createWebServiceTemplate;

  def execute() : String = {
    val source = new StreamSource(new StringReader(wwsInput.request))
    val writer = new StringWriter
    val streamResult = new StreamResult(writer)
    webServiceTemplate.sendSourceAndReceiveToResult(source, usernameTokenHandler, streamResult)

    return writer.toString
  }

  private def createWebServiceTemplate : WebServiceTemplate = {
    val wsTemplate = new WebServiceTemplate()
    wsTemplate.setDefaultUri(wwsInput.wssEndpoint)

    wsTemplate
  }

  private def usernameTokenHandler : UsernameTokenHandler = {
    new UsernameTokenHandler(wwsInput.username, wwsInput.password)
  }
}
