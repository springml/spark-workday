package com.springml.spark.workday.ws

import org.apache.log4j.Logger
import org.apache.wss4j.common.WSS4JConstants
import org.apache.wss4j.common.ext.WSSecurityException
import org.apache.wss4j.dom.WSConstants
import org.apache.wss4j.dom.message.{WSSecHeader, WSSecUsernameToken}
import org.springframework.ws.WebServiceMessage
import org.springframework.ws.client.core.WebServiceMessageCallback
import org.springframework.ws.soap.SoapMessage
import org.w3c.dom.Document
/**
  * Created by sam on 20/9/16.
  */
class UsernameTokenHandler(
                            val username: String,
                            val password: String
                          ) extends WebServiceMessageCallback {
  @transient val logger = Logger.getLogger(classOf[UsernameTokenHandler])

  override def doWithMessage(message: WebServiceMessage): Unit = {
    val doc = message.asInstanceOf[SoapMessage].getDocument

    val wSSecUsernameToken = new WSSecUsernameToken
    wSSecUsernameToken.setPasswordType(WSS4JConstants.PASSWORD_TEXT)
    wSSecUsernameToken.addNonce()
    wSSecUsernameToken.setUserInfo(username, password)

    val secHeader: WSSecHeader = new WSSecHeader(doc)
    secHeader.setMustUnderstand(false)
    try {
      secHeader.insertSecurityHeader
      val signedDoc: Document = wSSecUsernameToken.build(doc, secHeader)
      message.asInstanceOf[SoapMessage].setDocument(signedDoc)
    } catch {
      case e: WSSecurityException => {
        logger.error("Error while inserting WSSec Username token", e)
      }
    }
  }
}
