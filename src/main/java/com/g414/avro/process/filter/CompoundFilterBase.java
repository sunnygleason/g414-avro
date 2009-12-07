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
