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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.avro.generic.GenericRecord;

import com.g414.avro.process.ProcessingException;
import com.g414.avro.process.RecordHandler;

/**
 * A RecordHandler used for collecting distinct values from a collection of
 * records. Thread-safe.
 */
public class DistinctValues<T> implements RecordHandler {
	/** Collection of all values seen with counts */
	protected final ConcurrentHashMap<T, AtomicInteger> values = new ConcurrentHashMap<T, AtomicInteger>();

	/** field to look for in input */
	protected final String field;

	/**
	 * Creates a new instance that collects the specified field.
	 */
	public DistinctValues(String field) {
		this.field = field;
	}

	/** @see RecordHandler#start() */
	@Override
	public void start() {
	}

	/** @see RecordHandler#handle(GenericRecord) */
	@Override
	@SuppressWarnings("unchecked")
	public void handle(GenericRecord record) throws ProcessingException {
		T value = (T) record.get(field);

		AtomicInteger previous = values
				.putIfAbsent(value, new AtomicInteger(1));
		if (previous != null) {
			previous.incrementAndGet();
		}
	}

	/** @see RecordHandler#finish() */
	@Override
	public void finish() {
	}

	/**
	 * Returns the list of distinct values seen by this instance.
	 */
	public List<T> getDistinctValues() {
		List<T> outValues = new ArrayList<T>();
		outValues.addAll(values.keySet());

		return Collections.unmodifiableList(outValues);
	}

	/**
	 * Returns a map of instance values to the number of times the value was
	 * seen during processing.
	 */
	public Map<T, Integer> getValueCounts() {
		Map<T, Integer> outMap = new HashMap<T, Integer>();
		for (Map.Entry<T, AtomicInteger> entry : values.entrySet()) {
			outMap.put(entry.getKey(), entry.getValue().intValue());
		}

		return Collections.unmodifiableMap(outMap);
	}
}
