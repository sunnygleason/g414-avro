/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.g414.avro.process.filter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.g414.avro.process.RecordFilter;

/**
 * A RecordFilter implementation that consolidates input from multiple filters.
 * Thread-safe if the underlying delegates are Thread-safe.
 */
public abstract class CompoundFilterBase implements RecordFilter {
    /** collection of delegate filters */
    protected final List<RecordFilter> filters;

    /**
     * Construct a new instance of the compound filter with the specified
     * delegates.
     */
    public CompoundFilterBase(List<RecordFilter> filters) {
        List<RecordFilter> theFilters = new ArrayList<RecordFilter>();
        theFilters.addAll(filters);
        this.filters = Collections.unmodifiableList(filters);
    }

    /**
     * Abstract builder class for implementing subclasses.
     */
    public abstract static class CompoundFilterBuilder<T extends RecordFilter> {
        protected final List<T> filters = new ArrayList<T>();

        public CompoundFilterBuilder<T> add(T filter) {
            filters.add(filter);

            return this;
        }

        public abstract T build();
    }
}
