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

package org.apache.spark.sql.protobuf.utils

import org.apache.spark.sql.catalyst.expressions.{CreateMap, Expression, Hex, Literal, Unhex}
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.{MapType, StringType}
import org.apache.spark.unsafe.types.UTF8String

object ProtobufSqlUtils {
  def convertToString(e: Expression): String = e match {
    case Literal(v: UTF8String, StringType) => v.toString
    case _ => throw new IllegalArgumentException(
      s"Expected a string literal, got $e with class ${e.getClass} with type ${e.dataType}")
  }

  def readDescriptorFromExpression(e: Expression): Option[Array[Byte]] = e match {
    case Literal(v: UTF8String, StringType) =>
      if (v.toString.isEmpty) {
        None
      }
      else {
        Some(ProtobufUtils.readDescriptorFileContent(v.toString))
      }
    case Unhex(Literal(v: UTF8String, StringType), _) =>
      val vString = v.toString
      if (vString.isEmpty) {
        None
      }
      else {
        Some(Hex.unhex(v.getBytes))
      }
    case _ => throw new IllegalArgumentException(
      s"Type mismatched for the 3-rd argument, " +
        s"expected a string literal, or an unhex function on a string literal " +
        s"got $e with class ${e.getClass} with type ${e.dataType}")
  }

  def convertToMapData(exp: Expression): Map[String, String] = exp match {
    case m: CreateMap
      if m.dataType.acceptsType(MapType(StringType, StringType, valueContainsNull = false)) =>
      val arrayMap = m.eval().asInstanceOf[ArrayBasedMapData]
      ArrayBasedMapData.toScalaMap(arrayMap).map { case (key, value) =>
        key.toString -> value.toString
      }
    case m: CreateMap
      if m.keys.isEmpty =>
      Map.empty
    case m: CreateMap =>
      throw QueryCompilationErrors.keyValueInMapNotStringError(m)
    case _ =>
      throw QueryCompilationErrors.nonMapFunctionNotAllowedError
  }
}
