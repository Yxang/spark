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

import scala.reflect.ClassTag

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.{FUNC_ALIAS, FunctionBuilder}
import org.apache.spark.sql.catalyst.analysis.FunctionRegistryBase
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo}

class ProtobufSessionExtension extends (SparkSessionExtensions => Unit) {
  private def expression[T <: Expression: ClassTag](
    name: String,
    setAlias: Boolean = false,
    since: Option[String] = None
  ): (String, (ExpressionInfo, FunctionBuilder)) = {
    val (expressionInfo, builder) = FunctionRegistryBase.build[T](name, since)
    val newBuilder = (expressions: Seq[Expression]) => {
      val expr = builder(expressions)
      if (setAlias) expr.setTagValue(FUNC_ALIAS, name)
      expr
    }
    (name, (expressionInfo, newBuilder))
  }


  private val expressions: Map[String, (ExpressionInfo, FunctionBuilder)] = Map(
    expression[ProtobufDataToCatalyst]("from_protobuf"),
    expression[CatalystDataToProtobuf]("to_protobuf")
  )

  override def apply(extensions: SparkSessionExtensions): Unit =
    expressions.foreach { case (name, (info, builder)) =>
      extensions.injectFunction(FunctionIdentifier(name), info, builder)
    }
}
