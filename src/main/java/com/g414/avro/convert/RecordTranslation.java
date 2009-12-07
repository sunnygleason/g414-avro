package com.g414.avro.convert;

import org.apache.avro.generic.GenericRecord;

/**
 * Interface for translating records.
 */
public interface RecordTranslation {
	/** Translate the specified record to a new record. */
	public GenericRecord translate(GenericRecord in);
}
