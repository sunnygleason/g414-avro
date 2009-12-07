package com.g414.avro.process.handler;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;

import org.apache.avro.generic.GenericRecord;

import com.g414.avro.file.Tell;
import com.g414.avro.process.ProcessingException;
import com.g414.avro.process.RecordHandler;

/**
 * Indexes an Avro Input stream, recording the position of each record to the
 * specified outputstream. Not thread-safe because writing to the output stream
 * is not synchronized.
 */
public class DataIndexer implements RecordHandler {
	/** Tell to use as source of record positions */
	protected Tell reader;

	/** destination output stream */
	protected final OutputStream out;

	/** byte array to use for marshalling longs */
	protected final byte[] longBytes = new byte[8];

	/** LongBuffer to use for marshalling longs */
	protected final LongBuffer longBuf = ByteBuffer.wrap(longBytes)
			.asLongBuffer();

	/**
	 * Create a new instance that reads from the specified reader and writes to
	 * the specified output stream.
	 */
	public DataIndexer(Tell reader, OutputStream out) {
		this.reader = reader;
		this.out = out;
	}

	/** @see RecordHandler#start() */
	@Override
	public void start() {
	}

	/** @see RecordHandler#handle(GenericRecord) */
	@Override
	public void handle(GenericRecord record) throws ProcessingException {
		long pos = reader.lastPos();
		longBuf.put(pos);
		longBuf.rewind();

		try {
			out.write(longBytes);
		} catch (IOException e) {
			throw new ProcessingException("Error while writing index: "
					+ e.getMessage(), e);
		}
	}

	/** @see RecordHandler#finish() */
	@Override
	public void finish() {
		try {
			out.close();
		} catch (IOException ignored) {
		}
	}
}
