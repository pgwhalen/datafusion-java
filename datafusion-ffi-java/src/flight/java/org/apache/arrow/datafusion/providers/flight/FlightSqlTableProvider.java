package org.apache.arrow.datafusion.providers.flight;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.arrow.datafusion.common.DataFusionError;
import org.apache.arrow.datafusion.common.NativeDataFusionError;
import org.apache.arrow.datafusion.generated.DfError;
import org.apache.arrow.datafusion.generated.DfFlightSqlTableFactory;
import org.apache.arrow.datafusion.generated.DfRustTableProvider;
import org.apache.arrow.datafusion.providers.RustTableProvider;

/**
 * A {@code TableProvider} backed by a remote Flight SQL service.
 *
 * <p>This wraps {@code datafusion_table_providers::flight::FlightTableFactory} with the bundled
 * {@code FlightSqlDriver}. Each {@code FlightSqlTableProvider} corresponds to a single SQL query
 * baked in at construction time (via {@link Builder#query(String)}); the Flight service runs that
 * query and streams results back. For multi-table federation with filter / join pushdown, see
 * {@code FlightSqlFederatedCatalog} instead.
 *
 * {@snippet :
 * try (FlightSqlTableProvider taxi = FlightSqlTableProvider.builder()
 *         .endpoint("http://localhost:32010")
 *         .query("SELECT * FROM taxi_trips")
 *         .header("x-tenant", "acme")
 *         .build();
 *     SessionContext ctx = new SessionContext()) {
 *     ctx.registerTable("taxi", taxi);
 *     ctx.sql("SELECT count(*) FROM taxi").show();
 * }
 * }
 *
 * @see <a
 *     href="https://docs.rs/datafusion-table-providers/0.11.0/datafusion_table_providers/flight/struct.FlightTableFactory.html">Rust
 *     datafusion-table-providers: FlightTableFactory</a>
 */
public final class FlightSqlTableProvider extends RustTableProvider {

  /** Config-map key recognised by the bundled {@code FlightSqlDriver}. */
  public static final String QUERY_KEY = "flight.sql.query";

  public static final String USERNAME_KEY = "flight.sql.username";
  public static final String PASSWORD_KEY = "flight.sql.password";
  public static final String HEADER_PREFIX = "flight.sql.header.";

  private FlightSqlTableProvider(DfRustTableProvider handle) {
    super(handle);
  }

  /** Returns a new {@link Builder}. */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder for {@link FlightSqlTableProvider}. The {@code endpoint} and {@code query} are
   * required; all other options are optional.
   */
  public static final class Builder {
    private String endpoint;
    private final Map<String, String> options = new LinkedHashMap<>();

    private Builder() {}

    /** Required. Flight SQL endpoint URL, e.g. {@code http://host:32010}. */
    public Builder endpoint(String url) {
      this.endpoint = Objects.requireNonNull(url, "endpoint");
      return this;
    }

    /** Required. SQL query to execute on the remote Flight SQL service. */
    public Builder query(String sql) {
      options.put(QUERY_KEY, Objects.requireNonNull(sql, "query"));
      return this;
    }

    /** Optional. Username for basic-auth Handshake. */
    public Builder username(String username) {
      options.put(USERNAME_KEY, Objects.requireNonNull(username, "username"));
      return this;
    }

    /** Optional. Password for basic-auth Handshake. */
    public Builder password(String password) {
      options.put(PASSWORD_KEY, Objects.requireNonNull(password, "password"));
      return this;
    }

    /**
     * Optional. Sets a gRPC header on every Flight SQL request. The header name is sent as
     * {@code name.toLowerCase()} (gRPC convention).
     */
    public Builder header(String name, String value) {
      Objects.requireNonNull(name, "header name");
      Objects.requireNonNull(value, "header value");
      options.put(HEADER_PREFIX + name, value);
      return this;
    }

    /** Raw escape hatch: set an arbitrary option key accepted by the Flight SQL driver. */
    public Builder option(String key, String value) {
      Objects.requireNonNull(key, "option key");
      Objects.requireNonNull(value, "option value");
      options.put(key, value);
      return this;
    }

    /** Opens the Flight SQL connection and returns a ready-to-register provider. */
    public FlightSqlTableProvider build() {
      if (endpoint == null) {
        throw new IllegalStateException("endpoint(...) is required");
      }
      if (!options.containsKey(QUERY_KEY)) {
        throw new IllegalStateException("query(...) is required");
      }
      String[] keys = options.keySet().toArray(new String[0]);
      String[] values = options.values().toArray(new String[0]);
      try (DfFlightSqlTableFactory factory = new DfFlightSqlTableFactory()) {
        DfRustTableProvider handle = factory.openTable(endpoint, keys, values);
        return new FlightSqlTableProvider(handle);
      } catch (DfError e) {
        throw new NativeDataFusionError(e);
      } catch (RuntimeException e) {
        throw new DataFusionError("Flight SQL open_table failed", e);
      }
    }
  }
}
