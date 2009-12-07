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
