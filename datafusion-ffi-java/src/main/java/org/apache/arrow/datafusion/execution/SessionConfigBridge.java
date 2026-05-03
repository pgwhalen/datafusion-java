package org.apache.arrow.datafusion.execution;

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.arrow.datafusion.common.DataFusionError;
import org.apache.arrow.datafusion.common.NativeDataFusionError;
import org.apache.arrow.datafusion.generated.DfError;
import org.apache.arrow.datafusion.generated.DfSessionConfig;
import org.apache.arrow.datafusion.generated.DfStringArray;

/** Bridge between public {@link SessionConfig} API and Diplomat-generated DfSessionConfig. */
final class SessionConfigBridge implements AutoCloseable {
  private final DfSessionConfig df;
  private volatile boolean closed = false;

  SessionConfigBridge(DfSessionConfig df) {
    this.df = df;
  }

  static SessionConfigBridge fromOptionsMap(Map<String, String> options) {
    String[] keys = options.keySet().toArray(new String[0]);
    String[] values = options.values().toArray(new String[0]);
    try {
      return new SessionConfigBridge(DfSessionConfig.newFromOptions(keys, values));
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      throw new DataFusionError("Failed to create SessionConfig", e);
    }
  }

  DfSessionConfig dfSessionConfig() {
    if (closed) {
      throw new IllegalStateException("SessionConfig has been closed");
    }
    return df;
  }

  Map<String, String> options() {
    if (closed) {
      throw new IllegalStateException("SessionConfig has been closed");
    }
    try (DfStringArray keys = df.optionsKeys();
        DfStringArray values = df.optionsValues()) {
      int len = (int) keys.len();
      Map<String, String> out = new LinkedHashMap<>(len);
      for (int i = 0; i < len; i++) {
        out.put(keys.get(i), values.get(i));
      }
      return Map.copyOf(out);
    }
  }

  @Override
  public void close() {
    if (!closed) {
      closed = true;
      df.close();
    }
  }
}
