package org.apache.arrow.datafusion.common;

/**
 * A reference to a table, which may be unqualified (bare), partially qualified (schema + table), or
 * fully qualified (catalog + schema + table).
 *
 * <p>Example:
 *
 * <p>{@snippet : TableReference bare = new TableReference.Bare("users"); TableReference partial =
 * new TableReference.Partial("public", "users"); TableReference full = new TableReference.Full(
 * "my_catalog", "public", "users"); String table = full.table(); // "users" }
 *
 * @see <a
 *     href="https://docs.rs/datafusion-common/52.1.0/datafusion_common/enum.TableReference.html">Rust
 *     DataFusion: TableReference</a>
 */
public sealed interface TableReference {

  /**
   * An unqualified table reference consisting of only a table name.
   *
   * @param table the table name
   */
  record Bare(String table) implements TableReference {

    /**
     * Returns the table name.
     *
     * @return the table name
     * @see <a
     *     href="https://docs.rs/datafusion-common/52.1.0/datafusion_common/enum.TableReference.html#method.table">Rust
     *     DataFusion: TableReference::table</a>
     */
    public String table() {
      return table;
    }
  }

  /**
   * A partially qualified table reference consisting of a schema and table name.
   *
   * @param schema the schema name
   * @param table the table name
   */
  record Partial(String schema, String table) implements TableReference {

    /**
     * Returns the schema name.
     *
     * @return the schema name
     * @see <a
     *     href="https://docs.rs/datafusion-common/52.1.0/datafusion_common/enum.TableReference.html#method.schema">Rust
     *     DataFusion: TableReference::schema</a>
     */
    public String schema() {
      return schema;
    }

    /**
     * Returns the table name.
     *
     * @return the table name
     * @see <a
     *     href="https://docs.rs/datafusion-common/52.1.0/datafusion_common/enum.TableReference.html#method.table">Rust
     *     DataFusion: TableReference::table</a>
     */
    public String table() {
      return table;
    }
  }

  /**
   * A fully qualified table reference consisting of a catalog, schema, and table name.
   *
   * @param catalog the catalog name
   * @param schema the schema name
   * @param table the table name
   */
  record Full(String catalog, String schema, String table) implements TableReference {

    /**
     * Returns the catalog name.
     *
     * @return the catalog name
     * @see <a
     *     href="https://docs.rs/datafusion-common/52.1.0/datafusion_common/enum.TableReference.html#method.catalog">Rust
     *     DataFusion: TableReference::catalog</a>
     */
    public String catalog() {
      return catalog;
    }

    /**
     * Returns the schema name.
     *
     * @return the schema name
     * @see <a
     *     href="https://docs.rs/datafusion-common/52.1.0/datafusion_common/enum.TableReference.html#method.schema">Rust
     *     DataFusion: TableReference::schema</a>
     */
    public String schema() {
      return schema;
    }

    /**
     * Returns the table name.
     *
     * @return the table name
     * @see <a
     *     href="https://docs.rs/datafusion-common/52.1.0/datafusion_common/enum.TableReference.html#method.table">Rust
     *     DataFusion: TableReference::table</a>
     */
    public String table() {
      return table;
    }
  }
}
