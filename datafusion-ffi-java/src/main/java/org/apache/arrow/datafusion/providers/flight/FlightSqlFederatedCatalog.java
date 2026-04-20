package org.apache.arrow.datafusion.providers.flight;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.arrow.datafusion.common.DataFusionError;
import org.apache.arrow.datafusion.common.NativeDataFusionError;
import org.apache.arrow.datafusion.generated.DfError;
import org.apache.arrow.datafusion.generated.DfFlightSqlFederatedCatalogFactory;
import org.apache.arrow.datafusion.generated.DfRustCatalogProvider;
import org.apache.arrow.datafusion.providers.RustCatalogProvider;

/**
 * A federation-aware {@code CatalogProvider} backed by a remote Flight SQL service.
 *
 * <p>Each registered table flows through {@code datafusion_federation::sql::SQLFederationProvider}:
 * DataFusion unparses the sub-plan rooted at the table (including filters, projections, joins, and
 * aggregates) back into SQL and ships the generated SQL to the Flight SQL service via {@code
 * CommandStatementQuery}. Pushdown is automatic; there is no per-table query baked in at
 * registration time (in contrast to {@link FlightSqlTableProvider}).
 *
 * <p>Register with a {@linkplain
 * org.apache.arrow.datafusion.execution.SessionContext#newWithFederation federated SessionContext}
 * so the {@code FederationOptimizerRule} + {@code FederatedQueryPlanner} are available to recognise
 * the provider and rewrite plans.
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
 *     ctx.sql("SELECT count(*) FROM remote1.public.users WHERE active = true").show();
 * }
 * }
 *
 * @see <a
 *     href="https://docs.rs/datafusion-federation/0.5.3/datafusion_federation/sql/struct.SQLFederationProvider.html">Rust
 *     datafusion-federation: SQLFederationProvider</a>
 */
public final class FlightSqlFederatedCatalog extends RustCatalogProvider {

  /** Default schema name under which registered tables are exposed. */
  public static final String DEFAULT_SCHEMA = "public";

  /** Config-map key recognised by the Flight SQL Handshake. Same as {@link FlightSqlTableProvider}. */
  public static final String USERNAME_KEY = "flight.sql.username";

  public static final String PASSWORD_KEY = "flight.sql.password";
  public static final String HEADER_PREFIX = "flight.sql.header.";

  private FlightSqlFederatedCatalog(DfRustCatalogProvider handle) {
    super(handle);
  }

  /** Returns a new {@link Builder}. */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder for {@link FlightSqlFederatedCatalog}. {@code endpoint}, {@code computeContext}, and at
   * least one {@code table(...)} are required.
   */
  public static final class Builder {
    private String endpoint;
    private String computeContext;
    private String schemaName = DEFAULT_SCHEMA;
    private final Map<String, String> options = new LinkedHashMap<>();
    private final List<String> tables = new ArrayList<>();

    private Builder() {}

    /** Required. Flight SQL endpoint URL, e.g. {@code http://host:32010}. */
    public Builder endpoint(String url) {
      this.endpoint = Objects.requireNonNull(url, "endpoint");
      return this;
    }

    /**
     * Required. Stable, unique identifier for this remote. Federation only collapses sub-plans
     * that share a compute context, so two distinct remotes must have distinct values.
     */
    public Builder computeContext(String computeContext) {
      this.computeContext = Objects.requireNonNull(computeContext, "computeContext");
      return this;
    }

    /** Optional. Schema name under which the registered tables appear. Defaults to {@code public}. */
    public Builder schemaName(String schemaName) {
      this.schemaName = Objects.requireNonNull(schemaName, "schemaName");
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
     * Optional. Sets a gRPC header on every Flight SQL request. The header name is sent as {@code
     * name.toLowerCase()} (gRPC convention).
     */
    public Builder header(String name, String value) {
      Objects.requireNonNull(name, "header name");
      Objects.requireNonNull(value, "header value");
      options.put(HEADER_PREFIX + name, value);
      return this;
    }

    /** Raw escape hatch for arbitrary option keys accepted by the builder. */
    public Builder option(String key, String value) {
      Objects.requireNonNull(key, "option key");
      Objects.requireNonNull(value, "option value");
      options.put(key, value);
      return this;
    }

    /**
     * Registers a remote table. The name must resolve as a SQL identifier on the remote Flight SQL
     * service; the builder fetches the schema at {@link #build()} time by issuing a {@code SELECT *
     * FROM <name> WHERE 1 = 0} query.
     */
    public Builder table(String name) {
      tables.add(Objects.requireNonNull(name, "table name"));
      return this;
    }

    /** Opens the Flight SQL connection, fetches table schemas, and returns a ready catalog. */
    public FlightSqlFederatedCatalog build() {
      if (endpoint == null) {
        throw new IllegalStateException("endpoint(...) is required");
      }
      if (computeContext == null) {
        throw new IllegalStateException("computeContext(...) is required");
      }
      if (tables.isEmpty()) {
        throw new IllegalStateException("at least one table(...) is required");
      }
      String[] keys = options.keySet().toArray(new String[0]);
      String[] values = options.values().toArray(new String[0]);
      String[] tableArr = tables.toArray(new String[0]);
      try (DfFlightSqlFederatedCatalogFactory factory = new DfFlightSqlFederatedCatalogFactory()) {
        DfRustCatalogProvider handle =
            factory.build(endpoint, computeContext, schemaName, keys, values, tableArr);
        return new FlightSqlFederatedCatalog(handle);
      } catch (DfError e) {
        throw new NativeDataFusionError(e);
      } catch (RuntimeException e) {
        throw new DataFusionError("Flight SQL federated catalog build failed", e);
      }
    }
  }
}
