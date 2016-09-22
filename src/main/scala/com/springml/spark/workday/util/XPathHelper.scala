package com.springml.spark.workday.util

import java.io.{ByteArrayOutputStream, StringReader}
import javax.xml.transform.stream.StreamSource

import net.sf.saxon.lib.NamespaceConstant
import net.sf.saxon.s9api._
import org.apache.commons.lang3.CharEncoding
import org.apache.log4j.Logger

/**
  * This is same as XPathProcessor from spark-xml-utls.
  * But the difference is that it returns list of String instead of appending all strings into a single String
  */
class XPathHelper(
                 val namespaceMap : Map[String, String],
                 val featureMappings : Map[String, String]
                 ) {
  @transient val logger = Logger.getLogger(classOf[XPathHelper])

  @transient private var xsel : XPathSelector = null
  @transient val baos = new ByteArrayOutputStream()
  @transient var serializer : Serializer = null
  @transient var builder : DocumentBuilder = null

  private def init(xpath : String) {
    try {
      // Get the processor
      val proc = new Processor(false)
      // Set any specified configuration properties for the processor
      if (featureMappings != null) {
        import scala.collection.JavaConversions._
        for (entry <- featureMappings.entrySet) {
          proc.setConfigurationProperty(entry.getKey, entry.getValue)
        }
      }

      // Get the XPath compiler
      val xpathCompiler = proc.newXPathCompiler
      // Set the namespace to prefix mappings
      setPrefixNamespaceMappings(xpathCompiler, namespaceMap)
      // Compile the XPath expression  and get a document builder
      xsel = xpathCompiler.compile(xpath).load
      builder = proc.newDocumentBuilder
      // Create and initialize the serializer
      serializer = proc.newSerializer(baos)
      serializer.setOutputStream(baos)
      serializer.setOutputProperty(Serializer.Property.METHOD, "xml")
      serializer.setOutputProperty(Serializer.Property.OMIT_XML_DECLARATION, "yes")
      serializer.setProcessor(proc)
    }
    catch {
      case e: SaxonApiException => {
        throw new Exception(e.getMessage, e)
      }
    }
  }

  def evaluate(xpath : String, content : String) : List[String] = {
    init(xpath)

    // Prepare to evaluate the XPath expression against the content
    val source = new StreamSource(new StringReader(content))
    val xmlDoc = builder.build(source)
    xsel.setContextItem(xmlDoc)
    var resultList = List[String]()

    val results = xsel.evaluate()
    val iter = results.iterator()
    while (iter.hasNext) {
      val item = iter.next()
      resetSerializer
      serializer.serializeXdmValue(item)

      resultList.+:(new String(baos.toByteArray(), CharEncoding.UTF_8))
    }

    resultList
  }

  def evaluateToString(xpath : String, content : String) : String = {
    init(xpath)

    // Prepare to evaluate the XPath expression against the content
    val source = new StreamSource(new StringReader(content))
    val xmlDoc = builder.build(source)
    xsel.setContextItem(xmlDoc)
    resetSerializer

    val results = xsel.evaluate()
    val iter = results.iterator()
    while (iter.hasNext) {
      val item = iter.next()
      serializer.serializeXdmValue(item)

    }

    (new String(baos.toByteArray(), CharEncoding.UTF_8))
  }

  private def resetSerializer {
    //Reset the serializer
    serializer.close()
    baos.reset()
  }

  private def setPrefixNamespaceMappings(xpathCompiler: XPathCompiler,
                                         namespaceMappings: Map[String, String]) {
    for ((k,v) <- namespaceMappings) {
      xpathCompiler.declareNamespace(k, v)
    }

    // Add in the defaults
    xpathCompiler.declareNamespace("xml", NamespaceConstant.XML)
    xpathCompiler.declareNamespace("xs", NamespaceConstant.SCHEMA)
    xpathCompiler.declareNamespace("fn", NamespaceConstant.FN)
  }
}
