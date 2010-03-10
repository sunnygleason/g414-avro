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

import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.avro.generic.GenericRecord;
import org.codehaus.jackson.map.ObjectMapper;

import com.g414.avro.jackson.AvroObjectMapper;
import com.g414.avro.process.ProcessingException;
import com.g414.avro.process.RecordHandler;

/**
 * A trivial RecordHandler that writes all records to the specified writer as
 * JSON-encoded lines. Not thread-safe as writing to the writer is not known to
 * be thread-safe.
 */
public class JsonWriter implements RecordHandler {
	protected final ObjectMapper mapper = AvroObjectMapper.getObjectMapper();
	protected final PrintWriter writer;

	/**
	 * Construct an instance that writes to the specified writer using the
	 * specified delimiter.
	 */
	public JsonWriter(PrintWriter writer) {
		this.writer = writer;
	}

	/** @see DelimitedTextFileWriter#start() */
	@Override
	public void start() {
	}

	/** @see DelimitedTextFileWriter#handle(GenericRecord) */
	@Override
	public void handle(GenericRecord record) throws ProcessingException {
		try {
			StringWriter strWriter = new StringWriter();
			mapper.writeValue(strWriter, record);
			writer.println(strWriter.toString());
		} catch (Exception e) {
			throw new ProcessingException(e);
		}
	}

	/** @see DelimitedTextFileWriter#finish() */
	@Override
	public void finish() {
		writer.close();
	}
}
