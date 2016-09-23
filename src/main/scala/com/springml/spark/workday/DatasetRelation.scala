package com.springml.spark.workday

import java.math.BigDecimal
import java.sql.{Date, Timestamp}

import com.springml.spark.workday.util.XPathHelper
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

/**
  * DatasetRelation for Workday
  */
class DatasetRelation(
                       xmlRecords : List[String],
                       xpathMap : Map[String, String],
                       namespaceMap : Map[String, String],
                       sparkSqlContext : SQLContext,
                       userSchema : StructType
                     ) extends BaseRelation with TableScan {

  @transient val logger = Logger.getLogger(classOf[DatasetRelation])

  private val xPathHelper = new XPathHelper(namespaceMap, null)
  private val records = read()

  def read(row: String): scala.collection.mutable.Map[String, String]= {
    var record = scala.collection.mutable.Map[String, String]()
    for ((column, xpath) <- xpathMap) {
      val result = xPathHelper.evaluateToString(xpath, row)
      logger.debug("Xpath evaluation response for xpath " + xpath + " \n" + result)
      record(column) = result
      // TODO Handle details
    }

    record
  }

  private def read() : List[scala.collection.mutable.Map[String, String]] = {
    xmlRecords.map(row => read(row))
  }

  override def sqlContext: SQLContext = sparkSqlContext

  override def schema: StructType = {
    if (userSchema != null) {
      userSchema
    } else {
      // Construct the schema with all fields as String
      val firstRow = records(0)
      val structFields = new Array[StructField](firstRow.size)
      var index: Int = 0
      for (fieldEntry <- firstRow) {
        structFields(index) = StructField(fieldEntry._1, StringType, nullable = true)
        index = index + 1
      }

      StructType(structFields)
    }
  }

  override def buildScan(): RDD[Row] = {
    val schemaFields = schema.fields
    logger.info("Total records size : " + records.size)
    val rowArray = new Array[Row](records.size)
    var rowIndex: Int = 0
    for (row <- records) {
      val fieldArray = new Array[Any](schemaFields.length)
      logger.debug("Total Fields length : " + schemaFields.length)
      var fieldIndex: Int = 0
      for (fields <- schemaFields) {
        val value = fieldValue(row, fields.name)
        logger.debug("fieldValue " + value)
        fieldArray(fieldIndex) = cast(value, fields.dataType, fields.nullable, fields.name)
        fieldIndex = fieldIndex + 1
      }

      logger.debug("rowIndex : " + rowIndex)
      rowArray(rowIndex) = Row.fromSeq(fieldArray)
      rowIndex = rowIndex + 1
    }
    sqlContext.sparkContext.parallelize(rowArray)
  }

  private def cast(fieldValue: String, toType: DataType,
                   nullable: Boolean = true, fieldName: String): Any = {
    if (fieldValue == "" && nullable && !toType.isInstanceOf[StringType]) {
      null
    } else {
      toType match {
        case _: ByteType => fieldValue.toByte
        case _: ShortType => fieldValue.toShort
        case _: IntegerType => fieldValue.toInt
        case _: LongType => fieldValue.toLong
        case _: FloatType => fieldValue.toFloat
        case _: DoubleType => fieldValue.toDouble
        case _: BooleanType => fieldValue.toBoolean
        case _: DecimalType => new BigDecimal(fieldValue.replaceAll(",", ""))
        case _: TimestampType => Timestamp.valueOf(fieldValue)
        case _: DateType => Date.valueOf(fieldValue)
        case _: StringType => fieldValue
        case _ => throw new RuntimeException(s"Unsupported data type: ${toType.typeName}")
      }
    }
  }

  private def fieldValue(row: scala.collection.mutable.Map[String, String], name: String) : String = {
    if (row.contains(name)) {
      row(name)
    } else {
      logger.debug("Value not found for " + name)
      ""
    }
  }
}
