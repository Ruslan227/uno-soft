package org.example;

public enum ColumnState {
    DELIMITER,
    VALUE_WRAPPER_FIRST,
    VALUE,
    VALUE_WRAPPER_SECOND,
    NEW_LINE,
    SKIP_UNTIL_NEW_LINE
}
