package com.g414.avro.process;

import org.apache.avro.generic.GenericRecord;

/**
 * Interface for classes that process Records; RecordHandlers are typically
 * stateful and update state as new records are handled.
 */
public interface RecordHandler {
	/**
	 * Called by the RecordProcessor to inform this instance that processing is
	 * about to begin.
	 */
	public void start();

	/**
	 * Process a single record.
	 */
	public void handle(GenericRecord record) throws ProcessingException;

	/**
	 * Called by the RecordProcessor to inform this instance that processing has
	 * completed.
	 */
	public void finish();
}
