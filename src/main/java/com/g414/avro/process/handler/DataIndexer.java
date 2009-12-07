/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
