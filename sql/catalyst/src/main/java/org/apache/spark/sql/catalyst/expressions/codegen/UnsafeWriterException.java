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

package org.apache.spark.sql.catalyst.expressions.codegen;

import org.apache.spark.sql.catalyst.InternalRow;

/**
 * Exception thrown by UnsafeWriter when a NullPointerException occurs during row writing.
 * Carries the field ordinal and (optionally) the row that was being written, to support
 * opt-in NPE diagnostics via spark.sql.codegen.addNPEDebugCode.
 */
public class UnsafeWriterException extends RuntimeException {
    private final int ordinal;
    private InternalRow row;
    private String rowCsv;

    public UnsafeWriterException(String message, int ordinal, Exception cause) {
        super(message, cause);
        this.ordinal = ordinal;
    }

    public void setRow(InternalRow row) {
        this.row = row;
    }

    public InternalRow getRow() {
        return row;
    }

    public void setRowCsv(String data) {
        this.rowCsv = data;
    }

    public String getRowCsv() {
        return rowCsv;
    }

    public int getOrdinal() {
        return ordinal;
    }
}
