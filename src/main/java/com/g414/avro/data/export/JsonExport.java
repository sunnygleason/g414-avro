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
package com.g414.avro.data.export;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.zip.GZIPInputStream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;

import com.g414.avro.file.SequentialDataReader;
import com.g414.avro.process.RecordProcessor;
import com.g414.avro.process.handler.JsonWriter;

public class JsonExport {
	public static void main(String[] args) {
		LinkedList<String> theArgs = new LinkedList<String>();
		theArgs.addAll(Arrays.asList(args));
		Schema schema = null;

		String schemaName = theArgs.removeFirst();
		try {
			schema = Schema.parse(new File(schemaName));
		} catch (IOException e) {
			e.printStackTrace();
			System.err.println("Error reading schema : " + schemaName);
			System.exit(0);
		}

		for (String file : theArgs) {
			try {
				InputStream input = new FileInputStream(file);
				if (file.endsWith(".gz")) {
					input = new GZIPInputStream(input);
				}

				PrintWriter writer = new PrintWriter(file + ".out.json");
				RecordProcessor<GenericRecord> processor = new RecordProcessor<GenericRecord>(
						schema, null, new JsonWriter(writer));
				processor.process(new SequentialDataReader<GenericRecord>(
						schema, input, new GenericDatumReader<GenericRecord>(
								schema)));
				// close file...
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
