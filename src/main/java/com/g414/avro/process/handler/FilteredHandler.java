package com.g414.avro.process.handler;

import org.apache.avro.generic.GenericRecord;

import com.g414.avro.process.ProcessingException;
import com.g414.avro.process.RecordFilter;
import com.g414.avro.process.RecordHandler;

/**
 * RecordHandler instance that delegates matching records to a specified
 * handler. Thread-safe if the underlying delegates are thread-safe.
 */
public class FilteredHandler implements RecordHandler {
	/** filter to use */
	protected final RecordFilter filter;

	/** delegate handler */
	protected final RecordHandler handler;

	/**
	 * Create a new CompoundHandler that filters matching records to a specified
	 * handler.
	 */
	public FilteredHandler(RecordFilter filter, RecordHandler handler) {
		this.filter = filter;
		this.handler = handler;
	}

	/** @see RecordHandler#start() */
	@Override
	public void start() {
		handler.start();
	}

	/** @see RecordHandler#handle(GenericRecord) */
	@Override
	public void handle(GenericRecord record) throws ProcessingException {
		if (filter == null || filter.matches(record)) {
			handler.handle(record);
		}
	}

	/** @see RecordHandler#finish() */
	@Override
	public void finish() {
		handler.finish();
	}
}