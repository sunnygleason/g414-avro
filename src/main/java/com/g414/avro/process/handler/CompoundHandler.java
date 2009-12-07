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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.avro.generic.GenericRecord;

import com.g414.avro.process.ProcessingException;
import com.g414.avro.process.RecordHandler;

/**
 * RecordHandler instance that delegates to multiple handlers. Thread-safe if
 * the underlying delegates are thread-safe.
 */
public class CompoundHandler implements RecordHandler {
	/** collection of delegate handlers */
	protected final List<RecordHandler> handlers;

	/**
	 * Create a new CompoundHandler that delegates to the specified handlers.
	 */
	public CompoundHandler(List<RecordHandler> handlers) {
		List<RecordHandler> theHandlers = new ArrayList<RecordHandler>();
		theHandlers.addAll(handlers);
		this.handlers = Collections.unmodifiableList(handlers);
	}

	/** @see RecordHandler#start() */
	@Override
	public void start() {
		for (RecordHandler handler : handlers) {
			handler.start();
		}
	}

	/** @see RecordHandler#handle(GenericRecord) */
	@Override
	public void handle(GenericRecord record) throws ProcessingException {
		for (RecordHandler handler : handlers) {
			handler.handle(record);
		}
	}

	/** @see RecordHandler#finish() */
	@Override
	public void finish() {
		for (RecordHandler handler : handlers) {
			handler.finish();
		}
	}

	/**
	 * Builder class for creating CompoundHandler instances.
	 */
	public static class CompoundHandlerBuilder {
		protected List<RecordHandler> handlers = new ArrayList<RecordHandler>();

		public void add(RecordHandler handler) {
			handlers.add(handler);
		}

		public CompoundHandler build() {
			return new CompoundHandler(handlers);
		}
	}
}
