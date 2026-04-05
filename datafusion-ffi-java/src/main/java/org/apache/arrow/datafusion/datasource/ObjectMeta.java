package org.apache.arrow.datafusion.datasource;

/**
 * Metadata for an object in an object store, containing its location and size.
 *
 * <p>This type mirrors a subset of the Rust {@code object_store::ObjectMeta} struct. Currently only
 * {@code location} and {@code size} are exposed; additional fields ({@code last_modified}, {@code
 * e_tag}, {@code version}) may be added in the future.
 *
 * <p>Example:
 *
 * <p>{@snippet : ObjectMeta meta = new ObjectMeta("/data/file.parquet", 1024000); String location =
 * meta.location(); long size = meta.size(); System.out.println(location + " (" + size + " bytes)");
 * }
 *
 * @param location the path or URI of the object
 * @param size the object size in bytes
 * @see <a href="https://docs.rs/object_store/0.12.4/object_store/struct.ObjectMeta.html">Rust
 *     object_store: ObjectMeta</a>
 */
public record ObjectMeta(String location, long size) {

  /**
   * Returns the path or URI of the object.
   *
   * @return the object location
   * @see <a
   *     href="https://docs.rs/object_store/0.12.4/object_store/struct.ObjectMeta.html#structfield.location">Rust
   *     object_store: ObjectMeta::location</a>
   */
  public String location() {
    return location;
  }

  /**
   * Returns the object size in bytes.
   *
   * @return size in bytes
   * @see <a
   *     href="https://docs.rs/object_store/0.12.4/object_store/struct.ObjectMeta.html#structfield.size">Rust
   *     object_store: ObjectMeta::size</a>
   */
  public long size() {
    return size;
  }
}
