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
package com.g414.avro.process;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData.Record;

import com.g414.avro.file.SequentialDataReader;

/**
 * A driver class that processes records through a set of handlers and filters.
 * Right now, the processor is fail-fast, aborting processing if the given
 * handler throws an exception.
 */
public class RecordProcessor<D extends GenericRecord> {
    /** input schema */
    protected final Schema schema;

    /** record handler instance */
    protected final RecordHandler handler;

    /** record filter instance */
    protected final RecordFilter filter;

    /**
     * Constructs a new instance that uses the specified schema, filter, and
     * handler to process records.
     */
    public RecordProcessor(Schema schema, RecordFilter filter,
            RecordHandler handler) {
        this.schema = schema;
        this.filter = filter;
        this.handler = handler;
    }

    /**
     * Processes all records in the specified files (sequentially).
     */
    public void processFiles(List<String> files) throws ProcessingException {
        try {
            handler.start();

            for (String fname : files) {
                InputStream input = new FileInputStream(fname);
                if (fname.endsWith(".gz")) {
                    input = new GZIPInputStream(input);
                }

                SequentialDataReader<GenericRecord> reader = new SequentialDataReader<GenericRecord>(
                        schema, input, new GenericDatumReader<GenericRecord>(
                                schema));
                processImpl(reader);
            }

            handler.finish();
        } catch (IOException e) {
            throw new ProcessingException("Error while processing files: "
                    + e.getMessage(), e);
        }
    }

    /**
     * Processes the records from the given reader.
     */
    public void process(SequentialDataReader<GenericRecord> reader)
            throws ProcessingException {
        handler.start();
        processImpl(reader);
        handler.finish();
    }

    /**
     * Implements processing the records in a given reader.
     */
    protected void processImpl(SequentialDataReader<GenericRecord> reader)
            throws ProcessingException {
        Record record = new Record(schema);

        while (reader.next(record) != null) {
            if (handler != null && (filter == null || filter.matches(record))) {
                handler.handle(record);
                record = new Record(schema);
            }
        }
    }
}
