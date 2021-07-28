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

package org.apache.spark.sql.catalyst.expressions.aggregate

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types._

@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns the sum calculated from values of a group.")
case class Sum(child: Expression) extends DeclarativeAggregate with ImplicitCastInputTypes {

  override def children: Seq[Expression] = child :: Nil

  override def nullable: Boolean = child.nullable

  // Return data type.
  override def dataType: DataType = resultType

  override def inputTypes: Seq[AbstractDataType] = Seq(NumericType)

  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForNumericExpr(child.dataType, "function sum")

  private lazy val resultType = child.dataType match {
    case DecimalType.Fixed(precision, scale) =>
      DecimalType.bounded(precision + 10, scale)
    case _: IntegralType => LongType
    case _ => DoubleType
  }

  private lazy val sumDataType = resultType

  private lazy val sum = {
    if (child.nullable) AttributeReference("sum", sumDataType)()
    else AttributeReference("sum", sumDataType, nullable = false)()
  }

  private lazy val zero = Literal.default(resultType)

  override lazy val aggBufferAttributes = sum :: Nil

  override lazy val initialValues: Seq[Expression] = resultType match {
    case _ if nullable => Seq(Literal(null, resultType))
    case _ => Seq(zero)
  }

  override lazy val updateExpressions: Seq[Expression] = {
    if (child.nullable) {
      Seq(
        /* sum = */
        coalesce(coalesce(sum, zero) + child.cast(sumDataType), sum)
      )
    } else {
      Seq(
        /* sum = */
//        If(child.isNull, sum, sum + KnownNotNull(child).cast(resultType))
        sum + child.cast(resultType)
//        coalesce(sum, zero) + child.cast(sumDataType)
      )
    }
  }

  override lazy val mergeExpressions: Seq[Expression] = {
    if (child.nullable) {
      Seq(
        /* sum = */
        coalesce(coalesce(sum.left, zero) + sum.right, sum.left)
      )
    } else {
      Seq(
        /* sum = */
        sum.left + sum.right
      )
    }
  }

  override lazy val evaluateExpression: Expression = sum
}
