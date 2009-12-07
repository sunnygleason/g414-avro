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
