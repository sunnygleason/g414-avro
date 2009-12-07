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
 * Filters which may be useful when working with Strings. All of these filters
 * are thread-safe.
 */
public class StringFilters {
	/** Create a new filter that returns != null and length() > 0 */
	public RecordFilter getNotEmpty(final String field) {
		return new RecordFilter() {
			@Override
			public boolean matches(GenericRecord record) {
				Object other = record.get(field);

				return (other != null) && (other.toString().length() > 0);
			}
		};
	}

	/** Create a new filter that returns == null or length() > 0 */
	public RecordFilter getEmpty(final String field) {
		return new RecordFilter() {
			@Override
			public boolean matches(GenericRecord record) {
				Object other = record.get(field);

				return (other == null) || (other.toString().length() == 0);
			}
		};
	}

	/** Create a new filter that returns startsWith(value) */
	public RecordFilter getStartsWith(final String field, final String value,
			final boolean ignoreCase) {
		return new RecordFilter() {
			private final String theValue = ignoreCase ? value.toLowerCase()
					: value;

			@Override
			public boolean matches(GenericRecord record) {
				String other = record.get(field).toString();
				if (ignoreCase) {
					other = other.toLowerCase();
				}

				return other.startsWith(theValue);
			}
		};
	}

	/** Create a new filter that returns endsWith(value) */
	public RecordFilter getEndsWith(final String field, final String value,
			final boolean ignoreCase) {
		return new RecordFilter() {
			private final String theValue = ignoreCase ? value.toLowerCase()
					: value;

			@Override
			public boolean matches(GenericRecord record) {
				String other = record.get(field).toString();
				if (ignoreCase) {
					other = other.toLowerCase();
				}

				return other.endsWith(theValue);
			}
		};
	}

	/** Create a new filter that returns contains(value) */
	public RecordFilter getContains(final String field, final String value,
			final boolean ignoreCase) {
		return new RecordFilter() {
			private final String theValue = ignoreCase ? value.toLowerCase()
					: value;

			@Override
			public boolean matches(GenericRecord record) {
				String other = record.get(field).toString();
				if (ignoreCase) {
					other = other.toLowerCase();
				}

				return other.contains(theValue);
			}
		};
	}

	/** Create a new filter that returns compareTo(value) < 0 */
	public RecordFilter getLT(final String field, final String value,
			final boolean ignoreCase) {
		return new RecordFilter() {
			private final String theValue = ignoreCase ? value.toLowerCase()
					: value;

			@Override
			public boolean matches(GenericRecord record) {
				String other = record.get(field).toString();
				if (ignoreCase) {
					other = other.toLowerCase();
				}

				return other.compareTo(theValue) < 0;
			}
		};
	}

	/** Create a new filter that returns compareTo(value) <= 0 */
	public RecordFilter getLEQ(final String field, final String value,
			final boolean ignoreCase) {
		return new RecordFilter() {
			private final String theValue = ignoreCase ? value.toLowerCase()
					: value;

			@Override
			public boolean matches(GenericRecord record) {
				String other = record.get(field).toString();
				if (ignoreCase) {
					other = other.toLowerCase();
				}

				return other.compareTo(theValue) <= 0;
			}
		};
	}

	/** Create a new filter that returns compareTo(value) > 0 */
	public RecordFilter getGT(final String field, final String value,
			final boolean ignoreCase) {
		return new RecordFilter() {
			private final String theValue = ignoreCase ? value.toLowerCase()
					: value;

			@Override
			public boolean matches(GenericRecord record) {
				String other = record.get(field).toString();
				if (ignoreCase) {
					other = other.toLowerCase();
				}

				return other.compareTo(theValue) > 0;
			}
		};
	}

	/** Create a new filter that returns compareTo(value) >= 0 */
	public RecordFilter getGEQ(final String field, final String value,
			final boolean ignoreCase) {
		return new RecordFilter() {
			private final String theValue = ignoreCase ? value.toLowerCase()
					: value;

			@Override
			public boolean matches(GenericRecord record) {
				String other = record.get(field).toString();
				if (ignoreCase) {
					other = other.toLowerCase();
				}

				return other.compareTo(theValue) >= 0;
			}
		};
	}

	/** Create a new filter that returns equals(value) */
	public RecordFilter getEQ(final String field, final String value,
			final boolean ignoreCase) {
		return new RecordFilter() {
			private final String theValue = ignoreCase ? value.toLowerCase()
					: value;

			@Override
			public boolean matches(GenericRecord record) {
				String other = record.get(field).toString();
				if (ignoreCase) {
					other = other.toLowerCase();
				}

				return other.equals(theValue);
			}
		};
	}

	/** Create a new filter that returns !equals(value) */
	public RecordFilter getNE(final String field, final String value,
			final boolean ignoreCase) {
		return new RecordFilter() {
			private final String theValue = ignoreCase ? value.toLowerCase()
					: value;

			@Override
			public boolean matches(GenericRecord record) {
				String other = record.get(field).toString();
				if (ignoreCase) {
					other = other.toLowerCase();
				}

				return !other.equals(theValue);
			}
		};
	}
}
