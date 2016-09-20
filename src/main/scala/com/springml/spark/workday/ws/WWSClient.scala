package com.springml.spark.workday.ws

import java.io.{StringReader, StringWriter}
import javax.xml.transform.stream.{StreamResult, StreamSource}

import com.springml.spark.workday.model.WWSInput
import org.springframework.ws.client.core.WebServiceTemplate

/**
  * Created by sam on 20/9/16.
  */
class WWSClient(
               val wwsInput : WWSInput
               ) {

  def execute() : String = {
    val source = new StreamSource(new StringReader(wwsInput.request))
    val writer = new StringWriter
    val streamResult = new StreamResult(writer)
    webServiceTemplate.sendSourceAndReceiveToResult(source, usernameTokenHandler, streamResult)

    return writer.toString
  }

  private def webServiceTemplate : WebServiceTemplate = {
    val wsTemplate = new WebServiceTemplate()
    wsTemplate.setDefaultUri(wwsInput.wssEndpoint)

    wsTemplate
  }

  private def usernameTokenHandler : UsernameTokenHandler = {
    new UsernameTokenHandler(wwsInput.username, wwsInput.password)
  }
}
