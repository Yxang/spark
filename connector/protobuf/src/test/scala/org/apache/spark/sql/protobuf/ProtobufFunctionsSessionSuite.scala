/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.protobuf

import scala.collection.JavaConverters._

import com.google.protobuf.{ByteString, DynamicMessage}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, DataFrame, QueryTest}
import org.apache.spark.sql.functions.{lit, struct}
import org.apache.spark.sql.protobuf.protos.SimpleMessageProtos._
import org.apache.spark.sql.protobuf.protos.SimpleMessageProtos.SimpleMessageRepeated.NestedEnum
import org.apache.spark.sql.protobuf.utils.ProtobufUtils
import org.apache.spark.sql.test.SharedSparkSession

class ProtobufFunctionsSessionSuite extends QueryTest with SharedSparkSession with ProtobufTestBase
  with Serializable {

  import testImplicits._

  override def sparkConf: SparkConf = {
    super.sparkConf.set(
      "spark.sql.extensions", "org.apache.spark.sql.protobuf.ProtobufSessionExtension")
  }

  val testFileDescFile = protobufDescriptorFile("functions_suite.desc")
  private val testFileDesc = ProtobufUtils.readDescriptorFileContent(testFileDescFile)
  private val javaClassNamePrefix = "org.apache.spark.sql.protobuf.protos.SimpleMessageProtos$"

  val proto2FileDescFile = protobufDescriptorFile("proto2_messages.desc")
  val proto2FileDesc = ProtobufUtils.readDescriptorFileContent(proto2FileDescFile)
  private val proto2JavaClassNamePrefix = "org.apache.spark.sql.protobuf.protos.Proto2Messages$"

  private def emptyBinaryDF = Seq(Array[Byte]()).toDF("binary")

  /**
   * Runs the given closure twice. Once with descriptor file and second time with Java class name.
   */
  private def checkWithFileAndClassName(messageName: String)(
    fn: (String, Option[Array[Byte]]) => Unit): Unit = {
      withClue("(With descriptor file)") {
        fn(messageName, Some(testFileDesc))
      }
      withClue("(With Java class name)") {
        fn(s"$javaClassNamePrefix$messageName", None)
      }
  }

  private def checkWithFilePathAndClassName(messageName: String)(
    fn: (String, Option[String]) => Unit): Unit = {
    withClue("(With descriptor file)") {
      fn(messageName, Some(testFileDescFile))
    }
    withClue("(With Java class name)") {
      fn(s"$javaClassNamePrefix$messageName", None)
    }
  }

  private def checkWithProto2FileAndClassName(messageName: String)(
    fn: (String, Option[Array[Byte]]) => Unit): Unit = {
    withClue("(With descriptor file)") {
      fn(messageName, Some(proto2FileDesc))
    }
    withClue("(With Java class name)") {
      fn(s"$proto2JavaClassNamePrefix$messageName", None)
    }
  }

  // A wrapper to invoke the right variable of from_protobuf() depending on arguments.
  private def from_protobuf_wrapper(
    col: Column,
    messageName: String,
    descBytesOpt: Option[Array[Byte]],
    options: Map[String, String] = Map.empty): Column = {
    descBytesOpt match {
      case Some(descBytes) => functions.from_protobuf(
        col, messageName, descBytes, options.asJava
      )
      case None => functions.from_protobuf(col, messageName, options.asJava)
    }
  }

  // A wrapper to invoke the right variable of to_protobuf() depending on arguments.
  private def to_protobuf_wrapper(
    col: Column, messageName: String, descBytesOpt: Option[Array[Byte]]): Column = {
    descBytesOpt match {
      case Some(descBytes) => functions.to_protobuf(col, messageName, descBytes)
      case None => functions.to_protobuf(col, messageName)
    }
  }

  private def byte2string(bytes: Option[Array[Byte]]): String = {
    val string = bytes.map(_.map("%02X" format _).mkString).getOrElse("")
    s"unhex('$string')"
  }

  private def map2expression(Map: Map[String, String]): String = {
    val string = Map.map{case (k, v) => s"'$k', '$v'"}.mkString(", ")
    s"map($string)"
  }

  test("sql: roundtrip in to_protobuf and from_protobuf - struct") {
    val df = spark
      .range(1, 10)
      .select(struct(
        $"id",
        $"id".cast("string").as("string_value"),
        $"id".cast("int").as("int32_value"),
        $"id".cast("int").as("uint32_value"),
        $"id".cast("int").as("sint32_value"),
        $"id".cast("int").as("fixed32_value"),
        $"id".cast("int").as("sfixed32_value"),
        $"id".cast("long").as("int64_value"),
        $"id".cast("long").as("uint64_value"),
        $"id".cast("long").as("sint64_value"),
        $"id".cast("long").as("fixed64_value"),
        $"id".cast("long").as("sfixed64_value"),
        $"id".cast("double").as("double_value"),
        lit(1202.00).cast(org.apache.spark.sql.types.FloatType).as("float_value"),
        lit(true).as("bool_value"),
        lit("0".getBytes).as("bytes_value")).as("SimpleMessage"))

    df.createOrReplaceTempView("test_data")

    checkWithFileAndClassName("SimpleMessage") {
      case (name, descBytesOpt) =>
        val byteHexString = byte2string(descBytesOpt)
        val protoStructDF = spark.sql(
          s"""
             |select to_protobuf(SimpleMessage, '$name', $byteHexString) as proto
             |from test_data
             |""".stripMargin
        )
        protoStructDF.createOrReplaceTempView("protoStructDF")
        val actualDf = spark.sql(
          s"""
             |select from_protobuf(proto, '$name', $byteHexString) as proto
             |from protoStructDF
             |""".stripMargin
        )
        checkAnswer(actualDf, df)
    }
  }

  test("sql: roundtrip in from_protobuf and to_protobuf - Repeated") {

    val protoMessage = SimpleMessageRepeated
      .newBuilder()
      .setKey("key")
      .setValue("value")
      .addRboolValue(false)
      .addRboolValue(true)
      .addRdoubleValue(1092092.654d)
      .addRdoubleValue(1092093.654d)
      .addRfloatValue(10903.0f)
      .addRfloatValue(10902.0f)
      .addRnestedEnum(NestedEnum.NESTED_NOTHING)
      .addRnestedEnum(NestedEnum.NESTED_FIRST)
      .build()

    val df = Seq(protoMessage.toByteArray).toDF("value")
    df.createOrReplaceTempView("test_data")

    checkWithFileAndClassName("SimpleMessageRepeated") {
      case (name, descFilePathOpt) =>
        List(
          Map.empty[String, String],
          Map("enums.as.ints" -> "false"),
          Map("enums.as.ints" -> "true")).foreach(opts => {
          val fpExp = byte2string(descFilePathOpt)
          val optExp = map2expression(opts)
          val fromProtoDF = spark.sql(
            s"""
               |select from_protobuf(value, '$name', $fpExp, $optExp) as value_from
               |from test_data
               |""".stripMargin
          )
          fromProtoDF.createOrReplaceTempView("fromProtoDF")
          val toProtoDF = spark.sql(
            s"""
               |select to_protobuf(value_from, '$name', $fpExp) as value_to
               |from fromProtoDF
               |""".stripMargin
          )
          toProtoDF.createOrReplaceTempView("toProtoDF")
          val toFromProtoDF = spark.sql(
            s"""
               |select from_protobuf(value_to, '$name', $fpExp, $optExp) as value_to_from
               |from toProtoDF
               |""".stripMargin
          )
          checkAnswer(fromProtoDF.select($"value_from.*"), toFromProtoDF.select($"value_to_from.*"))
        })
    }
  }

  test("sql: roundtrip in from_protobuf and to_protobuf - Repeated - with desc file path") {

    val protoMessage = SimpleMessageRepeated
      .newBuilder()
      .setKey("key")
      .setValue("value")
      .addRboolValue(false)
      .addRboolValue(true)
      .addRdoubleValue(1092092.654d)
      .addRdoubleValue(1092093.654d)
      .addRfloatValue(10903.0f)
      .addRfloatValue(10902.0f)
      .addRnestedEnum(NestedEnum.NESTED_NOTHING)
      .addRnestedEnum(NestedEnum.NESTED_FIRST)
      .build()

    val df = Seq(protoMessage.toByteArray).toDF("value")
    df.createOrReplaceTempView("test_data")

    checkWithFilePathAndClassName("SimpleMessageRepeated") {
      case (name, descFilePathOpt) =>
        List(
          Map.empty[String, String],
          Map("enums.as.ints" -> "false"),
          Map("enums.as.ints" -> "true")).foreach(opts => {
          val fpExp = descFilePathOpt.getOrElse("")
          val optExp = map2expression(opts)
          val fromProtoDF = spark.sql(
            s"""
               |select from_protobuf(value, '$name', '$fpExp', $optExp) as value_from
               |from test_data
               |""".stripMargin
          )
          fromProtoDF.createOrReplaceTempView("fromProtoDF")
          val toProtoDF = spark.sql(
            s"""
               |select to_protobuf(value_from, '$name', '$fpExp') as value_to
               |from fromProtoDF
               |""".stripMargin
          )
          toProtoDF.createOrReplaceTempView("toProtoDF")
          val toFromProtoDF = spark.sql(
            s"""
               |select from_protobuf(value_to, '$name', '$fpExp', $optExp) as value_to_from
               |from toProtoDF
               |""".stripMargin
          )
          checkAnswer(fromProtoDF.select($"value_from.*"), toFromProtoDF.select($"value_to_from.*"))
        })
    }
  }

  test("sql: roundtrip in from_protobuf and to_protobuf - Repeated Message Once") {
    val repeatedMessageDesc = ProtobufUtils.buildDescriptor(testFileDesc, "RepeatedMessage")
    val basicMessageDesc = ProtobufUtils.buildDescriptor(testFileDesc, "BasicMessage")

    val basicMessage = DynamicMessage
      .newBuilder(basicMessageDesc)
      .setField(basicMessageDesc.findFieldByName("id"), 1111L)
      .setField(basicMessageDesc.findFieldByName("string_value"), "value")
      .setField(basicMessageDesc.findFieldByName("int32_value"), 12345)
      .setField(basicMessageDesc.findFieldByName("int64_value"), 0x90000000000L)
      .setField(basicMessageDesc.findFieldByName("double_value"), 10000000000.0d)
      .setField(basicMessageDesc.findFieldByName("float_value"), 10902.0f)
      .setField(basicMessageDesc.findFieldByName("bool_value"), true)
      .setField(
        basicMessageDesc.findFieldByName("bytes_value"),
        ByteString.copyFromUtf8("ProtobufDeserializer"))
      .build()

    val dynamicMessage = DynamicMessage
      .newBuilder(repeatedMessageDesc)
      .addRepeatedField(repeatedMessageDesc.findFieldByName("basic_message"), basicMessage)
      .build()

    val df = Seq(dynamicMessage.toByteArray).toDF("value")
    df.createOrReplaceTempView("test_data")

    checkWithFileAndClassName("RepeatedMessage") {
      case (name, descFilePathOpt) =>
        val fpExp = byte2string(descFilePathOpt)
        val fromProtoDF = spark.sql(
          s"""
             |select from_protobuf(value, '$name', $fpExp) as value_from
             |from test_data
             |""".stripMargin
        )
        fromProtoDF.createOrReplaceTempView("fromProtoDF")
        val toProtoDF = spark.sql(
          s"""
             |select to_protobuf(value_from, '$name', $fpExp) as value_to
             |from fromProtoDF
             |""".stripMargin
        )
        toProtoDF.createOrReplaceTempView("toProtoDF")
        val toFromProtoDF = spark.sql(
          s"""
             |select from_protobuf(value_to, '$name', $fpExp) as value_to_from
             |from toProtoDF
             |""".stripMargin
        )
        checkAnswer(fromProtoDF.select($"value_from.*"), toFromProtoDF.select($"value_to_from.*"))
    }
  }

  test("sql: roundtrip in from_protobuf and to_protobuf - Repeated Message Twice") {
    val repeatedMessageDesc = ProtobufUtils.buildDescriptor(testFileDesc, "RepeatedMessage")
    val basicMessageDesc = ProtobufUtils.buildDescriptor(testFileDesc, "BasicMessage")

    val basicMessage1 = DynamicMessage
      .newBuilder(basicMessageDesc)
      .setField(basicMessageDesc.findFieldByName("id"), 1111L)
      .setField(basicMessageDesc.findFieldByName("string_value"), "value1")
      .setField(basicMessageDesc.findFieldByName("int32_value"), 12345)
      .setField(basicMessageDesc.findFieldByName("int64_value"), 0x90000000000L)
      .setField(basicMessageDesc.findFieldByName("double_value"), 10000000000.0d)
      .setField(basicMessageDesc.findFieldByName("float_value"), 10902.0f)
      .setField(basicMessageDesc.findFieldByName("bool_value"), true)
      .setField(
        basicMessageDesc.findFieldByName("bytes_value"),
        ByteString.copyFromUtf8("ProtobufDeserializer1"))
      .build()
    val basicMessage2 = DynamicMessage
      .newBuilder(basicMessageDesc)
      .setField(basicMessageDesc.findFieldByName("id"), 1112L)
      .setField(basicMessageDesc.findFieldByName("string_value"), "value2")
      .setField(basicMessageDesc.findFieldByName("int32_value"), 12346)
      .setField(basicMessageDesc.findFieldByName("int64_value"), 0x90000000000L)
      .setField(basicMessageDesc.findFieldByName("double_value"), 10000000000.0d)
      .setField(basicMessageDesc.findFieldByName("float_value"), 10903.0f)
      .setField(basicMessageDesc.findFieldByName("bool_value"), false)
      .setField(
        basicMessageDesc.findFieldByName("bytes_value"),
        ByteString.copyFromUtf8("ProtobufDeserializer2"))
      .build()

    val dynamicMessage = DynamicMessage
      .newBuilder(repeatedMessageDesc)
      .addRepeatedField(repeatedMessageDesc.findFieldByName("basic_message"), basicMessage1)
      .addRepeatedField(repeatedMessageDesc.findFieldByName("basic_message"), basicMessage2)
      .build()

    val df = Seq(dynamicMessage.toByteArray).toDF("value")
    df.createOrReplaceTempView("test_data")

    checkWithFileAndClassName("RepeatedMessage") {
      case (name, descFilePathOpt) =>
        val fpExp = byte2string(descFilePathOpt)
        val fromProtoDF = spark.sql(
          s"""
             |select from_protobuf(value, '$name', $fpExp) as value_from
             |from test_data
             |""".stripMargin

        )
        fromProtoDF.createOrReplaceTempView("fromProtoDF")
        val toProtoDF = spark.sql(
          s"""
             |select to_protobuf(value_from, '$name', $fpExp) as value_to
             |from fromProtoDF
             |""".stripMargin
        )
        toProtoDF.createOrReplaceTempView("toProtoDF")
        val toFromProtoDF = spark.sql(
          s"""
             |select from_protobuf(value_to, '$name', $fpExp) as value_to_from
             |from toProtoDF
             |""".stripMargin
        )
        checkAnswer(fromProtoDF.select($"value_from.*"), toFromProtoDF.select($"value_to_from.*"))
    }
  }

  test("sql: roundtrip in from_protobuf and to_protobuf - Map") {
    val messageMapDesc = ProtobufUtils.buildDescriptor(testFileDesc, "SimpleMessageMap")

    val mapStr1 = DynamicMessage
      .newBuilder(messageMapDesc.findNestedTypeByName("StringMapdataEntry"))
      .setField(
        messageMapDesc.findNestedTypeByName("StringMapdataEntry").findFieldByName("key"),
        "string_key")
      .setField(
        messageMapDesc.findNestedTypeByName("StringMapdataEntry").findFieldByName("value"),
        "value1")
      .build()
    val mapStr2 = DynamicMessage
      .newBuilder(messageMapDesc.findNestedTypeByName("StringMapdataEntry"))
      .setField(
        messageMapDesc.findNestedTypeByName("StringMapdataEntry").findFieldByName("key"),
        "string_key")
      .setField(
        messageMapDesc.findNestedTypeByName("StringMapdataEntry").findFieldByName("value"),
        "value2")
      .build()
    val mapInt64 = DynamicMessage
      .newBuilder(messageMapDesc.findNestedTypeByName("Int64MapdataEntry"))
      .setField(
        messageMapDesc.findNestedTypeByName("Int64MapdataEntry").findFieldByName("key"),
        0x90000000000L)
      .setField(
        messageMapDesc.findNestedTypeByName("Int64MapdataEntry").findFieldByName("value"),
        0x90000000001L)
      .build()
    val mapInt32 = DynamicMessage
      .newBuilder(messageMapDesc.findNestedTypeByName("Int32MapdataEntry"))
      .setField(
        messageMapDesc.findNestedTypeByName("Int32MapdataEntry").findFieldByName("key"),
        12345)
      .setField(
        messageMapDesc.findNestedTypeByName("Int32MapdataEntry").findFieldByName("value"),
        54321)
      .build()
    val mapFloat = DynamicMessage
      .newBuilder(messageMapDesc.findNestedTypeByName("FloatMapdataEntry"))
      .setField(
        messageMapDesc.findNestedTypeByName("FloatMapdataEntry").findFieldByName("key"),
        "float_key")
      .setField(
        messageMapDesc.findNestedTypeByName("FloatMapdataEntry").findFieldByName("value"),
        109202.234f)
      .build()
    val mapDouble = DynamicMessage
      .newBuilder(messageMapDesc.findNestedTypeByName("DoubleMapdataEntry"))
      .setField(
        messageMapDesc.findNestedTypeByName("DoubleMapdataEntry").findFieldByName("key"),
        "double_key")
      .setField(
        messageMapDesc.findNestedTypeByName("DoubleMapdataEntry").findFieldByName("value"),
        109202.12d)
      .build()
    val mapBool = DynamicMessage
      .newBuilder(messageMapDesc.findNestedTypeByName("BoolMapdataEntry"))
      .setField(
        messageMapDesc.findNestedTypeByName("BoolMapdataEntry").findFieldByName("key"),
        true)
      .setField(
        messageMapDesc.findNestedTypeByName("BoolMapdataEntry").findFieldByName("value"),
        false)
      .build()

    val dynamicMessage = DynamicMessage
      .newBuilder(messageMapDesc)
      .setField(messageMapDesc.findFieldByName("key"), "key")
      .setField(messageMapDesc.findFieldByName("value"), "value")
      .addRepeatedField(messageMapDesc.findFieldByName("string_mapdata"), mapStr1)
      .addRepeatedField(messageMapDesc.findFieldByName("string_mapdata"), mapStr2)
      .addRepeatedField(messageMapDesc.findFieldByName("int64_mapdata"), mapInt64)
      .addRepeatedField(messageMapDesc.findFieldByName("int32_mapdata"), mapInt32)
      .addRepeatedField(messageMapDesc.findFieldByName("float_mapdata"), mapFloat)
      .addRepeatedField(messageMapDesc.findFieldByName("double_mapdata"), mapDouble)
      .addRepeatedField(messageMapDesc.findFieldByName("bool_mapdata"), mapBool)
      .build()

    val df = Seq(dynamicMessage.toByteArray).toDF("value")
    df.createOrReplaceTempView("test_data")

    checkWithFileAndClassName("SimpleMessageMap") {
      case (name, descFilePathOpt) =>
        val fpExp = byte2string(descFilePathOpt)
        val fromProtoDF = spark.sql(
          s"""
             |select from_protobuf(value, '$name', $fpExp) as value_from
             |from test_data
             |""".stripMargin
        )
        fromProtoDF.createOrReplaceTempView("fromProtoDF")
        val toProtoDF = spark.sql(
          s"""
             |select to_protobuf(value_from, '$name', $fpExp) as value_to
             |from fromProtoDF
             |""".stripMargin
        )
        toProtoDF.createOrReplaceTempView("toProtoDF")
        val toFromProtoDF = spark.sql(
          s"""
             |select from_protobuf(value_to, '$name', $fpExp) as value_to_from
             |from toProtoDF
             |""".stripMargin
        )
        checkAnswer(fromProtoDF.select($"value_from.*"), toFromProtoDF.select($"value_to_from.*"))
    }
  }


  def testFromProtobufWithOptions(
    df: DataFrame,
    expectedDf: DataFrame,
    options: java.util.HashMap[String, String],
    messageName: String): Unit = {
    val fromProtoDf = df.select(
      functions.from_protobuf($"value", messageName, testFileDesc, options) as 'sample)
    assert(expectedDf.schema === fromProtoDf.schema)
    checkAnswer(fromProtoDf, expectedDf)
  }
}
