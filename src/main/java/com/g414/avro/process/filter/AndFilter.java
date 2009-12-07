package com.g414.avro.process.filter;

import java.util.List;

import org.apache.avro.generic.GenericRecord;

import com.g414.avro.process.RecordFilter;

/**
 * A compound filter that matches if all of the delegating filters matches. This
 * implementation short-circuits for efficiency. Thread-safe if the underlying
 * delegates are Thread-safe.
 */
public class AndFilter extends CompoundFilterBase {
	/**
	 * Constructs a new instance that delegates to the specified filters.
	 */
	public AndFilter(List<RecordFilter> filters) {
		super(filters);
	}

	/** @see RecordFilter#matches(GenericRecord) */
	@Override
	public boolean matches(GenericRecord record) {
		for (RecordFilter filter : filters) {
			if (!filter.matches(record)) {
				return false;
			}
		}

		return true;
	}

	/** @see CompoundFilterBase.CompoundFilterBuilder */
	public static class AndFilterBuilder extends
			CompoundFilterBuilder<RecordFilter> {
		/** @see CompoundFilterBuilder#build() */
		@Override
		public AndFilter build() {
			return new AndFilter(filters);
		}
	}
}
