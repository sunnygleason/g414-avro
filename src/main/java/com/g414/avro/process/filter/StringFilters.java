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
