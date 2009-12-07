package com.g414.avro.process.filter;

import org.apache.avro.generic.GenericRecord;

import com.g414.avro.process.RecordFilter;

/**
 * A compound filter that matches if the specified filter doesn't match.
 */
public class NotFilter implements RecordFilter {
	/** delegate filter to be negated */
	protected final RecordFilter filter;

	/**
	 * Constructs a new instance that delegates to the specified filter.
	 */
	public NotFilter(RecordFilter filter) {
		this.filter = filter;
	}

	/** @see RecordFilter#matches(GenericRecord) */
	@Override
	public boolean matches(GenericRecord record) {
		return !filter.matches(record);
	}
}
