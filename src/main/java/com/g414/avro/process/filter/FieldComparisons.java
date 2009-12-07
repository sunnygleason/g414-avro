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
