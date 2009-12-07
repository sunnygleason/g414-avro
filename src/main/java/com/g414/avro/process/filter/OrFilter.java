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

import java.util.List;

import org.apache.avro.generic.GenericRecord;

import com.g414.avro.process.RecordFilter;
import com.g414.avro.process.filter.CompoundFilterBase.CompoundFilterBuilder;

/**
 * A compound filter that matches if any of the delegating filters matches. This
 * implementation short-circuits for efficiency. Thread-safe if the underlying
 * delegates are thread-safe.
 */
public class OrFilter extends CompoundFilterBase {
	/**
	 * Constructs a new instance that delegates to the specified filters.
	 */
	public OrFilter(List<RecordFilter> filters) {
		super(filters);
	}

	/** @see RecordFilter#matches(GenericRecord) */
	@Override
	public boolean matches(GenericRecord record) {
		for (RecordFilter filter : filters) {
			if (filter.matches(record)) {
				return true;
			}
		}

		return false;
	}

	/** @see CompoundFilterBase.CompoundFilterBuilder */
	public static class OrFilterBuilder extends
			CompoundFilterBuilder<RecordFilter> {
		/** @see CompoundFilterBuilder#build() */
		@Override
		public OrFilter build() {
			return new OrFilter(filters);
		}
	}
}
