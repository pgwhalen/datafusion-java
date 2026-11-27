/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datafusion;

/**
 * How a {@link TableProvider} handles a filter, returned per filter from {@link
 * TableProvider#supportsFiltersPushdown}. Mirrors DataFusion's {@code TableProviderFilterPushDown}
 * enum.
 */
public enum TableProviderFilterPushDown {
  /** The source does not apply this filter; DataFusion keeps it above the scan. */
  UNSUPPORTED,
  /** The source prunes candidates; DataFusion also applies the filter to the returned rows. */
  INEXACT,
  /** The source completely applies the filter with DataFusion-compatible semantics. */
  EXACT
}
