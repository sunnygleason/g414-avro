package com.g414.avro.process.handler;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.avro.generic.GenericRecord;

import com.g414.avro.process.ProcessingException;
import com.g414.avro.process.RecordHandler;

/**
 * RecordHandler for counting the number of records processed. Thread-safe.
 */
public class RecordCounter implements RecordHandler {
	/** the count */
	protected AtomicLong count = new AtomicLong();

	/** @see RecordHandler#start() */
	@Override
	public void start() {
	}

	/** @see RecordHandler#handle(GenericRecord) */
	@Override
	public void handle(GenericRecord record) throws ProcessingException {
		count.incrementAndGet();
	}

	/** @see RecordHandler#finish() */
	@Override
	public void finish() {
	}

	/** returns the count */
	public long getCount() {
		return this.count.get();
	}
}
