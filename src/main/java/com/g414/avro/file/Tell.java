package com.g414.avro.file;

/**
 * Interface for readers that have a previous position seen while processing
 * input. Exists because we don't want to expose SequentialDataReader to the
 * DataIndexer class.
 */
public interface Tell {
	/** returns the last position read */
	public long lastPos();
}
