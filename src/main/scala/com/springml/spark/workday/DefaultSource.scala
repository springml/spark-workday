package com.springml.spark.workday

import com.springml.spark.workday.model.{WWSInput, XPathInput}
import com.springml.spark.workday.util.CSVUtil
import org.apache.log4j.Logger
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

/**
  * Created by sam on 20/9/16.
  */
class DefaultSource extends RelationProvider with SchemaRelationProvider with CreatableRelationProvider {
  @transient val logger = Logger.getLogger(classOf[DefaultSource])

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation = {
    createRelation(sqlContext, parameters, null)
  }

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String],
                              schema: StructType): BaseRelation = {
    val username = param(parameters, "username")
    val password = param(parameters, "password")
    val wwsEndpoint = param(parameters, "wwsEndpoint")
    val objectTag = param(parameters, "objectTagPath")
    val detailsTag = param(parameters, "detailsTagPath")
    val request = param(parameters, "request")
    val xpath = param(parameters, "xpathMap")
    val namespacePrefix = parameters.get("namespacePrefixMap")

    val wwsInput = new WWSInput(username, password, wwsEndpoint, request)
    val xPathInput = new XPathInput(objectTag, detailsTag)
    CSVUtil.populateXPathInput(xpath, xPathInput)
    xPathInput.namespaceMap = CSVUtil.readCSV(namespacePrefix.get)
    logger.debug("Namespace Map" + xPathInput.namespaceMap)

    val records = new WWSReader(wwsInput, xPathInput) read()
    new DatasetRelation(records, sqlContext, schema)
  }

  override def createRelation(sqlContext: SQLContext,
                              mode: SaveMode,
                              parameters: Map[String, String],
                              data: DataFrame): BaseRelation = {
    logger.error("Save not supported by workday connector")
    throw new UnsupportedOperationException
  }

  private def param(parameters: Map[String, String],
                    paramName: String) : String = {
    val paramValue = parameters.getOrElse(paramName,
      sys.error(s"""'$paramName' must be specified for Spark Workday package"""))

    if ("password".equals(paramName)) {
      logger.debug("Param " + paramName + " value " + paramValue)
    }

    paramValue
  }

}
