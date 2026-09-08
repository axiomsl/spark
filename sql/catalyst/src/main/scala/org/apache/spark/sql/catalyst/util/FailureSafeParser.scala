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

package org.apache.spark.sql.catalyst.util

import org.apache.commons.lang3.exception.ExceptionUtils

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

class FailureSafeParser[IN](
    rawParser: IN => Iterable[InternalRow],
    mode: ParseMode,
    schema: StructType,
    columnNameOfCorruptRecord: String,
    columnNameOfCorruptRecordCause: String) {

  private val corruptFieldIndex = schema.getFieldIndex(columnNameOfCorruptRecord)
  // An empty name means the caller does not support a corrupt record cause column at all, so it
  // must be inert here: `StructField("", ...)` is a legal field, and matching it would strip a
  // field from `actualSchema` that the caller did not strip from the schema it gave `rawParser`.
  private val corruptErrorFieldName =
    Option(columnNameOfCorruptRecordCause).filter(_.nonEmpty)
  private val corruptErrorFieldIndex = corruptErrorFieldName.flatMap(schema.getFieldIndex)
  private val actualSchema = StructType(schema.filterNot(_.name == columnNameOfCorruptRecord)
    .filterNot(f => corruptErrorFieldName.contains(f.name)))
  private val resultRow = new GenericInternalRow(schema.length)
  private val nullResult = new GenericInternalRow(schema.length)

  // This function takes 2 parameters: an optional partial result, and the bad record. If the given
  // schema doesn't contain a field for corrupted record, we just return the partial result or a
  // row with all fields null. If the given schema contains a field for corrupted record, we will
  // set the bad record to this field, and set other fields according to the partial result or null.
  private val toResultRow: (Option[InternalRow], () => UTF8String,
    () => UTF8String) => InternalRow = {
    if (corruptFieldIndex.isDefined) {
      (row, badRecord, badRecordCause) => {
        var i = 0
        while (i < actualSchema.length) {
          val from = actualSchema(i)
          resultRow(schema.fieldIndex(from.name)) = row.map(_.get(i, from.dataType)).orNull
          i += 1
        }
        resultRow(corruptFieldIndex.get) = badRecord()
        if (corruptErrorFieldIndex.isDefined) {
          resultRow(corruptErrorFieldIndex.get) = badRecordCause()
        }
        resultRow
      }
    } else if (corruptErrorFieldIndex.isDefined) {
      // The cause column is stripped from `actualSchema` even when the corrupt record column is
      // absent, so a parsed row is one field shorter than `schema` and must be copied field by
      // field instead of being returned as is. The cause column then always reads as null, the
      // same surprise stock Spark already has for `columnNameOfCorruptRecord`.
      (row, _, _) => row.map { r =>
        var i = 0
        while (i < actualSchema.length) {
          val from = actualSchema(i)
          resultRow(schema.fieldIndex(from.name)) = r.get(i, from.dataType)
          i += 1
        }
        resultRow(corruptErrorFieldIndex.get) = null
        resultRow
      }.getOrElse(nullResult)
    } else {
      (row, _, _) => row.getOrElse(nullResult)
    }
  }

  def parse(input: IN): Iterator[InternalRow] = {
    try {
      rawParser.apply(input).iterator.map(row => toResultRow(Some(row), () => null, () => null))
    } catch {
      case e: BadRecordException => mode match {
        case PermissiveMode =>
          val badRecordCause = () => rootCauseString(e)
          val partialResults = e.partialResults()
          if (partialResults.nonEmpty) {
            partialResults.iterator.map(row => toResultRow(Some(row), e.record, badRecordCause))
          } else {
            Iterator(toResultRow(None, e.record, badRecordCause))
          }
        case DropMalformedMode =>
          Iterator.empty
        case FailFastMode =>
          e.getCause match {
            case _: JsonArraysAsStructsException =>
              // SPARK-42298 we recreate the exception here to make sure the error message
              // have the record content.
              throw QueryExecutionErrors.cannotParseJsonArraysAsStructsError(e.record().toString)
            case StringAsDataTypeException(fieldName, fieldValue, dataType) =>
              throw QueryExecutionErrors.cannotParseStringAsDataTypeError(e.record().toString,
                fieldName, fieldValue, dataType)
            case causeWrapper: LazyBadRecordCauseWrapper =>
              throwMalformedRecordsDetectedInRecordParsingError(e, causeWrapper.cause())
            case cause => throwMalformedRecordsDetectedInRecordParsingError(e, cause)
          }
      }
    }
  }

  private def throwMalformedRecordsDetectedInRecordParsingError(
      e: BadRecordException, cause: Throwable): Nothing = {
    throw QueryExecutionErrors.malformedRecordsDetectedInRecordParsingError(
      toResultRow(e.partialResults().headOption, e.record, () => null).toString, cause)
  }

  /**
   * Renders the root cause of a bad record as a string to be stored in the column named by
   * `columnNameOfCorruptRecordCause`.
   */
  private def rootCauseString(e: BadRecordException): UTF8String = {
    // `LazyBadRecordCauseWrapper` only carries a thunk and is not chained to the real cause, so it
    // has to be unwrapped first, the same way the FAILFAST branch above does.
    val cause = e.cause match {
      case causeWrapper: LazyBadRecordCauseWrapper => causeWrapper.cause()
      case other => other
    }
    if (cause != null) {
      val causeString = Option(ExceptionUtils.getRootCause(cause))
        .map(_.toString)
        .getOrElse(cause.toString)
      UTF8String.fromString(causeString)
    } else {
      UTF8String.fromString("NA")
    }
  }
}
