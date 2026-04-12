package org.apache.arrow.datafusion.logical_expr;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import org.apache.arrow.datafusion.common.TableReference;
import org.apache.arrow.datafusion.generated.DfLogicalPlanKind;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * A DataFusion logical plan node, corresponding to {@code datafusion::logical_expr::LogicalPlan}.
 *
 * <p>This sealed interface mirrors DataFusion's {@code LogicalPlan} enum. Each variant is a record
 * that holds the plan node's data. Use {@code instanceof} pattern matching to inspect plan
 * structure:
 *
 * {@snippet :
 * LogicalPlan plan = state.createLogicalPlan("SELECT * FROM t WHERE x > 1");
 * if (plan instanceof LogicalPlan.Projection proj) {
 *     System.out.println("Projecting " + proj.exprs().size() + " columns");
 *     if (proj.input() instanceof LogicalPlan.Filter filter) {
 *         System.out.println("Filter: " + filter.predicate());
 *     }
 * }
 * }
 *
 * @see <a
 *     href="https://docs.rs/datafusion/52.1.0/datafusion/logical_expr/enum.LogicalPlan.html">Rust
 *     DataFusion: LogicalPlan</a>
 */
public sealed interface LogicalPlan extends AutoCloseable {

  // ── Common methods ──

  /**
   * Returns the output schema of this plan node.
   *
   * {@snippet :
   * Schema schema = plan.schema();
   * }
   *
   * @return the Arrow schema
   * @see <a
   *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/logical_plan/plan/enum.LogicalPlan.html#method.schema">Rust
   *     DataFusion: LogicalPlan::schema</a>
   */
  Schema schema();

  /**
   * Returns the direct child input plans of this node.
   *
   * {@snippet :
   * List<LogicalPlan> children = plan.inputs();
   * }
   *
   * @return immutable list of child plans
   * @see <a
   *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/logical_plan/plan/enum.LogicalPlan.html#method.inputs">Rust
   *     DataFusion: LogicalPlan::inputs</a>
   */
  List<LogicalPlan> inputs();

  /**
   * Returns the underlying bridge. Internal use only.
   *
   * @return the bridge
   */
  LogicalPlanBridge bridge();

  /**
   * Single-line display of this plan node only (not the full tree).
   *
   * {@snippet :
   * String nodeDesc = plan.display();
   * }
   *
   * @return a single-line description
   * @see <a
   *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/logical_plan/plan/enum.LogicalPlan.html#method.display">Rust
   *     DataFusion: LogicalPlan::display</a>
   */
  default String display() {
    return bridge().displaySingle();
  }

  /**
   * Multi-line indented display of the full plan tree.
   *
   * {@snippet :
   * String tree = plan.displayIndent();
   * }
   *
   * @return the indented plan tree string
   * @see <a
   *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/logical_plan/plan/enum.LogicalPlan.html#method.display_indent">Rust
   *     DataFusion: LogicalPlan::display_indent</a>
   */
  default String displayIndent() {
    return bridge().displayIndent();
  }

  /**
   * Multi-line indented display with schema information.
   *
   * {@snippet :
   * String treeWithSchema = plan.displayIndentSchema();
   * }
   *
   * @return the indented plan tree string with schemas
   * @see <a
   *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/logical_plan/plan/enum.LogicalPlan.html#method.display_indent_schema">Rust
   *     DataFusion: LogicalPlan::display_indent_schema</a>
   */
  default String displayIndentSchema() {
    return bridge().displayIndentSchema();
  }

  /**
   * Graphviz DOT format display of the plan tree.
   *
   * {@snippet :
   * String dot = plan.displayGraphviz();
   * }
   *
   * @return the plan in DOT language format
   * @see <a
   *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/logical_plan/plan/enum.LogicalPlan.html#method.display_graphviz">Rust
   *     DataFusion: LogicalPlan::display_graphviz</a>
   */
  default String displayGraphviz() {
    return bridge().displayGraphviz();
  }

  /**
   * PostgreSQL-compatible JSON format display of the plan tree.
   *
   * {@snippet :
   * String json = plan.displayPgJson();
   * }
   *
   * @return the plan in PostgreSQL JSON format
   * @see <a
   *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/logical_plan/plan/enum.LogicalPlan.html#method.display_pg_json">Rust
   *     DataFusion: LogicalPlan::display_pg_json</a>
   */
  default String displayPgJson() {
    return bridge().displayPgJson();
  }

  /**
   * Maximum possible output rows, or empty if unknown.
   *
   * {@snippet :
   * OptionalLong max = plan.maxRows();
   * max.ifPresent(n -> System.out.println("At most " + n + " rows"));
   * }
   *
   * @return the maximum row count, or empty
   * @see <a
   *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/logical_plan/plan/enum.LogicalPlan.html#method.max_rows">Rust
   *     DataFusion: LogicalPlan::max_rows</a>
   */
  default OptionalLong maxRows() {
    return bridge().maxRows();
  }

  /**
   * Whether this plan contains references to outer columns (correlated subquery).
   *
   * {@snippet :
   * boolean correlated = plan.containsOuterReference();
   * }
   *
   * @return true if outer references exist
   * @see <a
   *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/logical_plan/plan/enum.LogicalPlan.html#method.contains_outer_reference">Rust
   *     DataFusion: LogicalPlan::contains_outer_reference</a>
   */
  default boolean containsOuterReference() {
    return bridge().containsOuterReference();
  }

  /** Closes this plan node and all its children, releasing native resources. */
  @Override
  default void close() {
    bridge().close();
    for (LogicalPlan child : inputs()) {
      child.close();
    }
  }

  // ── Factory ──

  /**
   * Creates a LogicalPlan sealed interface variant from a bridge.
   *
   * <p>This recursively materializes the entire plan tree.
   *
   * @param bridge the bridge wrapping the native plan
   * @return the appropriate variant record
   */
  static LogicalPlan fromBridge(LogicalPlanBridge bridge) {
    DfLogicalPlanKind kind = bridge.kind();
    Schema schema = bridge.schema();
    return switch (kind) {
      case PROJECTION -> {
        List<Expr> exprs = bridge.expressions();
        LogicalPlan input = fromBridge(bridge.inputAt(0));
        yield new Projection(exprs, input, schema, bridge);
      }
      case FILTER -> {
        Expr predicate = bridge.filterPredicate();
        LogicalPlan input = fromBridge(bridge.inputAt(0));
        yield new Filter(predicate, input, schema, bridge);
      }
      case WINDOW -> {
        List<Expr> windowExprs = bridge.windowExprs();
        LogicalPlan input = fromBridge(bridge.inputAt(0));
        yield new Window(input, windowExprs, schema, bridge);
      }
      case AGGREGATE -> {
        List<Expr> groupExprs = bridge.aggregateGroupExprs();
        List<Expr> aggrExprs = bridge.aggregateAggrExprs();
        LogicalPlan input = fromBridge(bridge.inputAt(0));
        yield new Aggregate(input, groupExprs, aggrExprs, schema, bridge);
      }
      case SORT -> {
        List<SortExpr> sortExprs = bridge.sortExprs();
        OptionalLong fetch = bridge.sortFetch();
        LogicalPlan input = fromBridge(bridge.inputAt(0));
        yield new Sort(sortExprs, input, fetch, schema, bridge);
      }
      case JOIN -> {
        LogicalPlan left = fromBridge(bridge.inputAt(0));
        LogicalPlan right = fromBridge(bridge.inputAt(1));
        JoinType joinType = bridge.joinType();
        JoinConstraint joinConstraint = bridge.joinConstraint();
        List<Expr> onLeftKeys = bridge.joinOnLeftKeys();
        List<Expr> onRightKeys = bridge.joinOnRightKeys();
        Optional<Expr> filter = bridge.joinFilter();
        NullEquality nullEquality = bridge.joinNullEquality();
        yield new Join(
            left,
            right,
            joinType,
            joinConstraint,
            onLeftKeys,
            onRightKeys,
            filter,
            nullEquality,
            schema,
            bridge);
      }
      case REPARTITION -> {
        LogicalPlan input = fromBridge(bridge.inputAt(0));
        yield new Repartition(input, schema, bridge);
      }
      case UNION -> {
        int count = bridge.inputsCount();
        List<LogicalPlan> inputs = new java.util.ArrayList<>(count);
        for (int i = 0; i < count; i++) {
          inputs.add(fromBridge(bridge.inputAt(i)));
        }
        yield new Union(List.copyOf(inputs), schema, bridge);
      }
      case TABLE_SCAN -> {
        TableReference tableName = bridge.tableScanTableName();
        Optional<List<Integer>> projection = bridge.tableScanProjection();
        List<Expr> filters = bridge.tableScanFilters();
        OptionalLong fetch = bridge.tableScanFetch();
        yield new TableScan(tableName, projection, filters, fetch, schema, bridge);
      }
      case EMPTY_RELATION -> {
        boolean produceOneRow = bridge.emptyRelationProduceOneRow();
        yield new EmptyRelation(produceOneRow, schema, bridge);
      }
      case SUBQUERY -> {
        LogicalPlan subquery = fromBridge(bridge.inputAt(0));
        yield new Subquery(subquery, schema, bridge);
      }
      case SUBQUERY_ALIAS -> {
        String alias = bridge.subqueryAliasName();
        LogicalPlan input = fromBridge(bridge.inputAt(0));
        yield new SubqueryAlias(input, alias, schema, bridge);
      }
      case LIMIT -> {
        Optional<Expr> skipExpr = bridge.limitSkip();
        Optional<Expr> fetchExpr = bridge.limitFetch();
        LogicalPlan input = fromBridge(bridge.inputAt(0));
        yield new Limit(input, skipExpr, fetchExpr, schema, bridge);
      }
      case STATEMENT -> new Statement(schema, bridge);
      case VALUES -> {
        List<List<Expr>> values = bridge.valuesExprs();
        yield new Values(values, schema, bridge);
      }
      case EXPLAIN -> {
        boolean verbose = bridge.explainVerbose();
        LogicalPlan plan = fromBridge(bridge.inputAt(0));
        yield new Explain(plan, verbose, schema, bridge);
      }
      case ANALYZE -> {
        boolean verbose = bridge.analyzeVerbose();
        LogicalPlan input = fromBridge(bridge.inputAt(0));
        yield new Analyze(input, verbose, schema, bridge);
      }
      case EXTENSION -> new Extension(schema, bridge);
      case DISTINCT -> {
        boolean isOn = bridge.distinctIsOn();
        if (isOn) {
          List<Expr> onExprs = bridge.distinctOnOnExprs();
          List<Expr> selectExprs = bridge.distinctOnSelectExprs();
          Optional<List<SortExpr>> sortExprs = bridge.distinctOnSortExprs();
          LogicalPlan input = fromBridge(bridge.inputAt(0));
          yield new Distinct.On(onExprs, selectExprs, sortExprs, input, schema, bridge);
        } else {
          LogicalPlan input = fromBridge(bridge.inputAt(0));
          yield new Distinct.All(input, schema, bridge);
        }
      }
      case DML -> {
        int count = bridge.inputsCount();
        List<LogicalPlan> inputs = count > 0 ? List.of(fromBridge(bridge.inputAt(0))) : List.of();
        yield new Dml(inputs, schema, bridge);
      }
      case DDL -> {
        int count = bridge.inputsCount();
        List<LogicalPlan> inputs = new java.util.ArrayList<>(count);
        for (int i = 0; i < count; i++) {
          inputs.add(fromBridge(bridge.inputAt(i)));
        }
        yield new Ddl(List.copyOf(inputs), schema, bridge);
      }
      case COPY -> {
        LogicalPlan input = fromBridge(bridge.inputAt(0));
        yield new Copy(input, schema, bridge);
      }
      case DESCRIBE_TABLE -> new DescribeTable(schema, bridge);
      case UNNEST -> {
        LogicalPlan input = fromBridge(bridge.inputAt(0));
        yield new Unnest(input, schema, bridge);
      }
      case RECURSIVE_QUERY -> {
        String name = bridge.recursiveQueryName();
        boolean isDistinct = bridge.recursiveQueryIsDistinct();
        LogicalPlan staticTerm = fromBridge(bridge.inputAt(0));
        LogicalPlan recursiveTerm = fromBridge(bridge.inputAt(1));
        yield new RecursiveQuery(name, staticTerm, recursiveTerm, isDistinct, schema, bridge);
      }
    };
  }

  // ══════════════════════════════════════════════════════════════════════════
  // Variant records
  // ══════════════════════════════════════════════════════════════════════════

  /**
   * Evaluates an arbitrary list of expressions (SELECT with expression list).
   *
   * @see <a
   *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/logical_plan/plan/struct.Projection.html">Rust
   *     DataFusion: Projection</a>
   */
  record Projection(List<Expr> exprs, LogicalPlan input, Schema schema, LogicalPlanBridge bridge)
      implements LogicalPlan {
    @Override
    public List<LogicalPlan> inputs() {
      return List.of(input);
    }
  }

  /**
   * Filters rows that do not match a predicate (WHERE clause).
   *
   * @see <a
   *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/logical_plan/plan/struct.Filter.html">Rust
   *     DataFusion: Filter</a>
   */
  record Filter(Expr predicate, LogicalPlan input, Schema schema, LogicalPlanBridge bridge)
      implements LogicalPlan {
    @Override
    public List<LogicalPlan> inputs() {
      return List.of(input);
    }
  }

  /**
   * Window function computation (OVER clause).
   *
   * @see <a
   *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/logical_plan/plan/struct.Window.html">Rust
   *     DataFusion: Window</a>
   */
  record Window(LogicalPlan input, List<Expr> windowExprs, Schema schema, LogicalPlanBridge bridge)
      implements LogicalPlan {
    @Override
    public List<LogicalPlan> inputs() {
      return List.of(input);
    }
  }

  /**
   * Groups and aggregates data (GROUP BY).
   *
   * @see <a
   *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/logical_plan/plan/struct.Aggregate.html">Rust
   *     DataFusion: Aggregate</a>
   */
  record Aggregate(
      LogicalPlan input,
      List<Expr> groupExprs,
      List<Expr> aggrExprs,
      Schema schema,
      LogicalPlanBridge bridge)
      implements LogicalPlan {
    @Override
    public List<LogicalPlan> inputs() {
      return List.of(input);
    }
  }

  /**
   * Sorts input per sort expressions (ORDER BY).
   *
   * @see <a
   *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/logical_plan/plan/struct.Sort.html">Rust
   *     DataFusion: Sort</a>
   */
  record Sort(
      List<SortExpr> sortExprs,
      LogicalPlan input,
      OptionalLong fetch,
      Schema schema,
      LogicalPlanBridge bridge)
      implements LogicalPlan {
    @Override
    public List<LogicalPlan> inputs() {
      return List.of(input);
    }
  }

  /**
   * Joins two relations on join conditions.
   *
   * @see <a
   *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/logical_plan/plan/struct.Join.html">Rust
   *     DataFusion: Join</a>
   */
  record Join(
      LogicalPlan left,
      LogicalPlan right,
      JoinType joinType,
      JoinConstraint joinConstraint,
      List<Expr> onLeftKeys,
      List<Expr> onRightKeys,
      Optional<Expr> filter,
      NullEquality nullEquality,
      Schema schema,
      LogicalPlanBridge bridge)
      implements LogicalPlan {
    @Override
    public List<LogicalPlan> inputs() {
      return List.of(left, right);
    }
  }

  /**
   * Repartitions input for parallelism.
   *
   * @see <a
   *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/logical_plan/plan/struct.Repartition.html">Rust
   *     DataFusion: Repartition</a>
   */
  record Repartition(LogicalPlan input, Schema schema, LogicalPlanBridge bridge)
      implements LogicalPlan {
    @Override
    public List<LogicalPlan> inputs() {
      return List.of(input);
    }
  }

  /**
   * Unions multiple inputs with the same schema (UNION ALL).
   *
   * @see <a
   *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/logical_plan/plan/struct.Union.html">Rust
   *     DataFusion: Union</a>
   */
  record Union(List<LogicalPlan> unionInputs, Schema schema, LogicalPlanBridge bridge)
      implements LogicalPlan {
    @Override
    public List<LogicalPlan> inputs() {
      return unionInputs;
    }
  }

  /**
   * Reads rows from a table (FROM clause).
   *
   * @see <a
   *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/logical_plan/plan/struct.TableScan.html">Rust
   *     DataFusion: TableScan</a>
   */
  record TableScan(
      TableReference tableName,
      Optional<List<Integer>> projection,
      List<Expr> filters,
      OptionalLong fetch,
      Schema schema,
      LogicalPlanBridge bridge)
      implements LogicalPlan {
    @Override
    public List<LogicalPlan> inputs() {
      return List.of();
    }
  }

  /**
   * Produces 0 or 1 placeholder rows (empty FROM clause).
   *
   * @see <a
   *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/logical_plan/plan/struct.EmptyRelation.html">Rust
   *     DataFusion: EmptyRelation</a>
   */
  record EmptyRelation(boolean produceOneRow, Schema schema, LogicalPlanBridge bridge)
      implements LogicalPlan {
    @Override
    public List<LogicalPlan> inputs() {
      return List.of();
    }
  }

  /**
   * Executes a nested query (subquery).
   *
   * @see <a
   *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/logical_plan/plan/struct.Subquery.html">Rust
   *     DataFusion: Subquery</a>
   */
  record Subquery(LogicalPlan subquery, Schema schema, LogicalPlanBridge bridge)
      implements LogicalPlan {
    @Override
    public List<LogicalPlan> inputs() {
      return List.of(subquery);
    }
  }

  /**
   * Provides or changes the name of a relation (AS alias).
   *
   * @see <a
   *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/logical_plan/plan/struct.SubqueryAlias.html">Rust
   *     DataFusion: SubqueryAlias</a>
   */
  record SubqueryAlias(LogicalPlan input, String alias, Schema schema, LogicalPlanBridge bridge)
      implements LogicalPlan {
    @Override
    public List<LogicalPlan> inputs() {
      return List.of(input);
    }
  }

  /**
   * Skips rows then fetches a limited count (LIMIT/OFFSET).
   *
   * @see <a
   *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/logical_plan/plan/struct.Limit.html">Rust
   *     DataFusion: Limit</a>
   */
  record Limit(
      LogicalPlan input,
      Optional<Expr> skipExpr,
      Optional<Expr> fetchExpr,
      Schema schema,
      LogicalPlanBridge bridge)
      implements LogicalPlan {
    @Override
    public List<LogicalPlan> inputs() {
      return List.of(input);
    }
  }

  /**
   * A non-relational SQL statement (SET, TRANSACTION, PREPARE, EXECUTE).
   *
   * <p>Sub-variant details are not yet exposed. See {@code LOGICAL_PLAN_MISSING_FEATURES.md}.
   *
   * @see <a
   *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/logical_plan/statement/enum.Statement.html">Rust
   *     DataFusion: Statement</a>
   */
  record Statement(Schema schema, LogicalPlanBridge bridge) implements LogicalPlan {
    @Override
    public List<LogicalPlan> inputs() {
      return List.of();
    }
  }

  /**
   * Literal row values (VALUES clause).
   *
   * @see <a
   *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/logical_plan/plan/struct.Values.html">Rust
   *     DataFusion: Values</a>
   */
  record Values(List<List<Expr>> values, Schema schema, LogicalPlanBridge bridge)
      implements LogicalPlan {
    @Override
    public List<LogicalPlan> inputs() {
      return List.of();
    }
  }

  /**
   * Shows plan structure (EXPLAIN).
   *
   * @see <a
   *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/logical_plan/plan/struct.Explain.html">Rust
   *     DataFusion: Explain</a>
   */
  record Explain(LogicalPlan plan, boolean verbose, Schema schema, LogicalPlanBridge bridge)
      implements LogicalPlan {
    @Override
    public List<LogicalPlan> inputs() {
      return List.of(plan);
    }
  }

  /**
   * Executes plan and prints physical plan with metrics (EXPLAIN ANALYZE).
   *
   * @see <a
   *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/logical_plan/plan/struct.Analyze.html">Rust
   *     DataFusion: Analyze</a>
   */
  record Analyze(LogicalPlan input, boolean verbose, Schema schema, LogicalPlanBridge bridge)
      implements LogicalPlan {
    @Override
    public List<LogicalPlan> inputs() {
      return List.of(input);
    }
  }

  /**
   * Custom relational operation defined outside DataFusion.
   *
   * <p>UserDefinedLogicalNode access is not yet exposed. See {@code
   * LOGICAL_PLAN_MISSING_FEATURES.md}.
   *
   * @see <a
   *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/logical_plan/plan/struct.Extension.html">Rust
   *     DataFusion: Extension</a>
   */
  record Extension(Schema schema, LogicalPlanBridge bridge) implements LogicalPlan {
    @Override
    public List<LogicalPlan> inputs() {
      return List.of();
    }
  }

  /**
   * Removes duplicate rows (SELECT DISTINCT).
   *
   * <p>This is a sealed sub-interface with two variants: {@link All} for plain DISTINCT and {@link
   * On} for PostgreSQL-style DISTINCT ON.
   *
   * @see <a
   *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/logical_plan/plan/enum.Distinct.html">Rust
   *     DataFusion: Distinct</a>
   */
  sealed interface Distinct extends LogicalPlan {

    /**
     * Plain DISTINCT referencing all selection expressions.
     *
     * @see <a
     *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/logical_plan/plan/enum.Distinct.html#variant.All">Rust
     *     DataFusion: Distinct::All</a>
     */
    record All(LogicalPlan input, Schema schema, LogicalPlanBridge bridge) implements Distinct {
      @Override
      public List<LogicalPlan> inputs() {
        return List.of(input);
      }
    }

    /**
     * PostgreSQL-style DISTINCT ON with separate control over DISTINCT'd and selected columns.
     *
     * @see <a
     *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/logical_plan/plan/struct.DistinctOn.html">Rust
     *     DataFusion: DistinctOn</a>
     */
    record On(
        List<Expr> onExprs,
        List<Expr> selectExprs,
        Optional<List<SortExpr>> sortExprs,
        LogicalPlan input,
        Schema schema,
        LogicalPlanBridge bridge)
        implements Distinct {
      @Override
      public List<LogicalPlan> inputs() {
        return List.of(input);
      }
    }
  }

  /**
   * Data Manipulation Language operations (INSERT, UPDATE, DELETE).
   *
   * <p>Sub-variant details are not yet exposed. See {@code LOGICAL_PLAN_MISSING_FEATURES.md}.
   *
   * @see <a
   *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/logical_plan/dml/enum.DmlStatement.html">Rust
   *     DataFusion: DmlStatement</a>
   */
  record Dml(List<LogicalPlan> dmlInputs, Schema schema, LogicalPlanBridge bridge)
      implements LogicalPlan {
    @Override
    public List<LogicalPlan> inputs() {
      return dmlInputs;
    }
  }

  /**
   * Data Definition Language operations (CREATE, DROP).
   *
   * <p>Sub-variant details are not yet exposed. See {@code LOGICAL_PLAN_MISSING_FEATURES.md}.
   *
   * @see <a
   *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/logical_plan/ddl/enum.DdlStatement.html">Rust
   *     DataFusion: DdlStatement</a>
   */
  record Ddl(List<LogicalPlan> ddlInputs, Schema schema, LogicalPlanBridge bridge)
      implements LogicalPlan {
    @Override
    public List<LogicalPlan> inputs() {
      return ddlInputs;
    }
  }

  /**
   * COPY TO for writing plan results to files.
   *
   * @see <a
   *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/logical_plan/dml/struct.CopyTo.html">Rust
   *     DataFusion: CopyTo</a>
   */
  record Copy(LogicalPlan input, Schema schema, LogicalPlanBridge bridge) implements LogicalPlan {
    @Override
    public List<LogicalPlan> inputs() {
      return List.of(input);
    }
  }

  /**
   * Describes table schema (DESCRIBE command).
   *
   * @see <a
   *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/logical_plan/plan/struct.DescribeTable.html">Rust
   *     DataFusion: DescribeTable</a>
   */
  record DescribeTable(Schema schema, LogicalPlanBridge bridge) implements LogicalPlan {
    @Override
    public List<LogicalPlan> inputs() {
      return List.of();
    }
  }

  /**
   * Unnests nested list/struct type columns (UNNEST).
   *
   * @see <a
   *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/logical_plan/plan/struct.Unnest.html">Rust
   *     DataFusion: Unnest</a>
   */
  record Unnest(LogicalPlan input, Schema schema, LogicalPlanBridge bridge) implements LogicalPlan {
    @Override
    public List<LogicalPlan> inputs() {
      return List.of(input);
    }
  }

  /**
   * Recursive CTEs (Common Table Expressions).
   *
   * @see <a
   *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/logical_plan/plan/struct.RecursiveQuery.html">Rust
   *     DataFusion: RecursiveQuery</a>
   */
  record RecursiveQuery(
      String name,
      LogicalPlan staticTerm,
      LogicalPlan recursiveTerm,
      boolean isDistinct,
      Schema schema,
      LogicalPlanBridge bridge)
      implements LogicalPlan {
    @Override
    public List<LogicalPlan> inputs() {
      return List.of(staticTerm, recursiveTerm);
    }
  }
}
