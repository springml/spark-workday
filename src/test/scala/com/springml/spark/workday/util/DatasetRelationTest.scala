package com.springml.spark.workday.util

import com.springml.spark.workday.DatasetRelation
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterEach, FunSuite}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by sam on 10/10/16.
  */
class DatasetRelationTest extends FunSuite with MockitoSugar with BeforeAndAfterEach {
  val STR_FIELD_NAME = "TextField"
  val STR_FIELD_VALUE = "TextFieldValue"
  val INT_FIELD_NAME = "IntegerField"
  val INT_FIELD_INT_VALUE = 100
  val INT_FIELD_STR_VALUE = "100"

  var sparkConf: SparkConf = _
  var sc: SparkContext = _
  var datasetRelation : DatasetRelation = _

  override def beforeEach() {
    sparkConf = new SparkConf().setMaster("local").setAppName("Test Dataset Relation")
    sc = new SparkContext(sparkConf)
  }

  private def getRecords : List[mutable.Map[String, String]] = {
    var resultListBuf = new ListBuffer[mutable.Map[String, String]]()
    resultListBuf += getRecord()

    resultListBuf.toList
  }

  private def getRecord(): mutable.Map[String, String] = {
    mutable.Map(STR_FIELD_NAME -> STR_FIELD_VALUE, INT_FIELD_NAME -> INT_FIELD_STR_VALUE)
  }

  private def getCustomSchema() : StructType = {
    val strField = StructField(STR_FIELD_NAME, StringType, true)
    val intField = StructField(INT_FIELD_NAME, IntegerType, true)

    val fields = Array[StructField] (strField, intField)
    StructType(fields)
  }

  override def afterEach() {
    sc.stop()
  }

  test("Test Build Scan") {
    val sQLContext = new SQLContext(sc)
    val datasetRelation = new DatasetRelation(getRecords, sQLContext, null)

    val rdd = datasetRelation.buildScan()
    validate(rdd)
  }

  test("Test Generated Schema") {
    val sQLContext = new SQLContext(sc)
    val datasetRelation = new DatasetRelation(getRecords, sQLContext, null)

    val schema = datasetRelation.schema
    assert(schema != null)

    val strField = schema(STR_FIELD_NAME)
    assert(StringType == strField.dataType)

    // By default all fields are String
    val intField = schema(INT_FIELD_NAME)
    assert(StringType == intField.dataType)
  }

  test("Test Build Scan With Custom Schema") {
    val sQLContext = new SQLContext(sc)
    val datasetRelation = new DatasetRelation(getRecords, sQLContext, getCustomSchema)

    val rdd = datasetRelation.buildScan()

    assert(rdd != null)
    assert(rdd.count() == 1l)
    val arr = rdd.collect()
    val actualRecord = arr(0)
    assert(actualRecord != null)
    val strValue = actualRecord.get(0)
    assert(strValue.isInstanceOf[String])
    assert(strValue.asInstanceOf[String].equals(STR_FIELD_VALUE))
    val intValue = actualRecord.get(1)
    assert(intValue.isInstanceOf[Integer])
    assert(intValue.asInstanceOf[Integer] == INT_FIELD_INT_VALUE)
  }

  test("Test With Custom Schema") {
    val sQLContext = new SQLContext(sc)
    val datasetRelation = new DatasetRelation(getRecords, sQLContext, getCustomSchema)

    val customSchema = datasetRelation.schema
    assert(customSchema != null)
    assert(customSchema.equals(getCustomSchema))
  }

  private def validate(rdd: RDD[Row]) {
    assert(rdd != null)
    assert(rdd.count() == 1l)
    val arr = rdd.collect()
    val actualRecord = arr.apply(0)
    assert(actualRecord != null)
    print(actualRecord.mkString)
    assert(actualRecord.mkString.contains(INT_FIELD_STR_VALUE))
  }

}
