package org.apache.spark.sql.catalyst.expressions.codegen;

import org.apache.spark.sql.catalyst.InternalRow;

public class UnsafeWriterException extends RuntimeException {
    private final int ordinal;
    private InternalRow row;
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

    public int getOrdinal() {
        return ordinal;
    }
}
