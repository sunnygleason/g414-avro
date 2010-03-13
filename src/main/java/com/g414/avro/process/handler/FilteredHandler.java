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