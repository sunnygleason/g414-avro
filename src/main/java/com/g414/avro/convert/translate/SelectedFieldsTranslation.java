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
package com.g414.avro.convert.translate;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData.Record;

import com.g414.avro.convert.RecordTranslation;

/**
 * RecordTranslation that maps selected fields from one record to another using
 * a specified translation map.
 */
public class SelectedFieldsTranslation implements RecordTranslation {
	/** schema to use */
	protected final Schema schema;

	/** map of in field names to out field names */
	protected final Map<String, String> fieldMap;

	/**
	 * Constructs a new instance using the specified schema and map of in field
	 * names to out field names.
	 */
	public SelectedFieldsTranslation(Schema schema, Map<String, String> fieldMap) {
		this.schema = schema;

		Map<String, String> theFields = new LinkedHashMap<String, String>();
		theFields.putAll(fieldMap);
		this.fieldMap = Collections.unmodifiableMap(theFields);
	}

	/** @see RecordTranslation#translate() */
	@Override
	public GenericRecord translate(GenericRecord in) {
		Record record = new Record(schema);

		for (Map.Entry<String, String> entry : fieldMap.entrySet()) {
			record.put(entry.getValue(), in.get(entry.getKey()));
		}

		return record;
	}
}
