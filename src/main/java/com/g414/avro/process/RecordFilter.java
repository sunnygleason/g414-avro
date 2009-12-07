package com.g414.avro.process;

import org.apache.avro.generic.GenericRecord;

/**
 * A RecordFilter is used to decide which records are processed by a
 * RecordProcessor instance (for example).
 */
public interface RecordFilter {
	/** Returns true if the filter matches the record, false otherwise. */
	public boolean matches(GenericRecord record);
}
