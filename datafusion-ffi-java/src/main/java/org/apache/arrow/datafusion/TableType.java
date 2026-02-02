package org.apache.arrow.datafusion;

/** The type of a table in a DataFusion catalog. */
public enum TableType {
  /** A base table (physical storage). */
  BASE(0),
  /** A view (virtual table defined by a query). */
  VIEW(1),
  /** A temporary table that exists only for the session. */
  TEMPORARY(2);

  private final int value;

  TableType(int value) {
    this.value = value;
  }

  /**
   * Gets the FFI value for this table type.
   *
   * @return The integer value matching FFI_TableType
   */
  public int getValue() {
    return value;
  }

  /**
   * Converts an FFI value to a TableType.
   *
   * @param value The FFI integer value
   * @return The corresponding TableType
   * @throws IllegalArgumentException if the value is not valid
   */
  public static TableType fromValue(int value) {
    for (TableType type : values()) {
      if (type.value == value) {
        return type;
      }
    }
    throw new IllegalArgumentException("Unknown TableType value: " + value);
  }
}
