package org.apache.arrow.datafusion.providers;

import org.apache.arrow.datafusion.generated.DfRustCatalogProvider;

/**
 * Handle for a {@code CatalogProvider} constructed on the Rust side.
 *
 * <p>Instances are produced by per-backend factory classes (for example, {@link
 * org.apache.arrow.datafusion.providers.flight.FlightSqlFederatedCatalog}) and registered with a
 * session via {@code SessionContext.registerCatalog(String, RustCatalogProvider)}. Unlike the
 * user-implementable {@link org.apache.arrow.datafusion.catalog.CatalogProvider} interface, a
 * {@code RustCatalogProvider} requires no Java-side callbacks — the backing catalog lives entirely
 * in Rust.
 *
 * {@snippet :
 * try (FlightSqlFederatedCatalog remote = FlightSqlFederatedCatalog.builder()
 *         .endpoint("http://localhost:32010")
 *         .computeContext("remote1")
 *         .table("users")
 *         .table("orders")
 *         .build();
 *     SessionContext ctx = SessionContext.newWithFederation(ConfigOptions.defaults())) {
 *     ctx.registerCatalog("remote1", remote);
 *     ctx.sql("SELECT * FROM remote1.public.users WHERE id = 1").show();
 * }
 * }
 *
 * <p>Instances must be {@link #close() closed} exactly once. Registering a provider with a session
 * does not transfer ownership; callers still close the handle when no longer needed.
 *
 * @see <a
 *     href="https://docs.rs/datafusion/53.1.0/datafusion/catalog/trait.CatalogProvider.html">Rust
 *     DataFusion: CatalogProvider</a>
 */
public abstract class RustCatalogProvider implements AutoCloseable {

  private final DfRustCatalogProvider handle;

  /** Protected — only subclasses (per-backend factories) construct instances. */
  protected RustCatalogProvider(DfRustCatalogProvider handle) {
    this.handle = handle;
  }

  /**
   * Internal accessor used by {@code SessionContextBridge} to call the Rust-side registration. Not
   * intended for application code; treat the returned opaque as internal.
   */
  public DfRustCatalogProvider handle() {
    return handle;
  }

  @Override
  public void close() {
    handle.close();
  }
}
