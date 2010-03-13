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

import org.apache.avro.generic.GenericRecord;

import com.g414.avro.process.ProcessingException;
import com.g414.avro.process.RecordHandler;

/**
 * A trivial RecordHandler that writes all records to the specified writer as
 * lines delimited by the specified delimiter. Note - this writer does not do
 * text field qualification, thus delimiter should be chosen wisely. Not
 * thread-safe as writing to the writer is not known to be thread-safe.
 */
public class DelimitedTextFileWriter implements RecordHandler {
    protected final PrintWriter writer;
    protected final String delim;

    /**
     * Construct an instance that writes to the specified writer using the
     * specified delimiter.
     */
    public DelimitedTextFileWriter(PrintWriter writer, String delim) {
        this.writer = writer;
        this.delim = delim;
    }

    /** @see DelimitedTextFileWriter#start() */
    @Override
    public void start() {
    }

    /** @see DelimitedTextFileWriter#handle(GenericRecord) */
    @Override
    public void handle(GenericRecord record) throws ProcessingException {
        int size = record.getSchema().getFields().size();

        StringBuilder builder = new StringBuilder();

        for (int i = 0; i < size; i++) {
            builder.append(delim);
            builder.append(record.get(i).toString());
        }

        String outStr = (size > 0) ? builder.substring(1) : "";
        writer.println(outStr);
    }

    /** @see DelimitedTextFileWriter#finish() */
    @Override
    public void finish() {
        writer.close();
    }
}
