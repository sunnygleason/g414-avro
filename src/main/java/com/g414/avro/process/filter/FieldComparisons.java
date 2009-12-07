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

import org.apache.avro.generic.GenericRecord;

import com.g414.avro.process.RecordFilter;

/**
 * A bunch of comparison filters that should be useful when processing records.
 * All of these filters are thread-safe.
 */
@SuppressWarnings("unchecked")
public class FieldComparisons {
	/** Create a filter that returns equals(value). */
	public <T> RecordFilter getEQ(final String field, final T value) {
		return new RecordFilter() {
			@Override
			public boolean matches(GenericRecord record) {
				T other = (T) record.get(field);

				return other.equals(value);
			}
		};
	}

	/** Create a filter that returns ! equals(value). */
	public <T> RecordFilter getNE(final String field, final T value) {
		return new RecordFilter() {
			@Override
			public boolean matches(GenericRecord record) {
				T other = (T) record.get(field);

				return !other.equals(value);
			}
		};
	}

	/** Create a filter that returns compareTo(value) >= 0 */
	public <T extends Comparable<T>> RecordFilter getGEQ(final String field,
			final T value) {
		return new RecordFilter() {
			@Override
			public boolean matches(GenericRecord record) {
				T other = (T) record.get(field);

				return other.compareTo(value) >= 0;
			}
		};
	}

	/** Create a filter that returns compareTo(value) > 0 */
	public <T extends Comparable<T>> RecordFilter getGT(final String field,
			final T value) {
		return new RecordFilter() {
			@Override
			public boolean matches(GenericRecord record) {
				T other = (T) record.get(field);

				return other.compareTo(value) > 0;
			}
		};
	}

	/** Create a filter that returns compareTo(value) <= 0 */
	public <T extends Comparable<T>> RecordFilter getLEQ(final String field,
			final T value) {
		return new RecordFilter() {
			@Override
			public boolean matches(GenericRecord record) {
				T other = (T) record.get(field);

				return other.compareTo(value) <= 0;
			}
		};
	}

	/** Create a filter that returns compareTo(value) < 0 */
	public <T extends Comparable<T>> RecordFilter getLT(final String field,
			final T value) {
		return new RecordFilter() {
			@Override
			public boolean matches(GenericRecord record) {
				T other = (T) record.get(field);

				return other.compareTo(value) < 0;
			}
		};
	}

	/** Create a filter that returns == null */
	public RecordFilter getIsNull(final String field) {
		return new RecordFilter() {
			@Override
			public boolean matches(GenericRecord record) {
				return record.get(field) == null;
			}
		};
	}

	/** Create a filter that returns != null */
	public RecordFilter getNotNull(final String field) {
		return new RecordFilter() {
			@Override
			public boolean matches(GenericRecord record) {
				return record.get(field) != null;
			}
		};
	}
}
