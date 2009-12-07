package com.g414.avro.process.handler;

import java.io.IOException;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;

import com.g414.avro.process.ProcessingException;
import com.g414.avro.process.RecordHandler;

/**
 * A RecordHandler that writes all records to the specified writer. Not
 * thread-safe as writing to the writer is not known to be thread-safe.
 */
public class RecordWriter implements RecordHandler {
	protected final DataFileWriter<GenericRecord> writer;

	/**
	 * Construct an instance that writes to the specified writer.
	 */
	public RecordWriter(DataFileWriter<GenericRecord> writer) {
		this.writer = writer;
	}

	/** @see RecordWriter#start() */
	@Override
	public void start() {
	}

	/** @see RecordWriter#handle(GenericRecord) */
	@Override
	public void handle(GenericRecord record) throws ProcessingException {
		try {
			writer.append(record);
		} catch (IOException e) {
			throw new ProcessingException("Error while writing record: "
					+ e.getMessage(), e);
		}
	}

	/** @see RecordWriter#finish() */
	@Override
	public void finish() {
		try {
			writer.close();
		} catch (IOException ignored) {
		}
	}
}
