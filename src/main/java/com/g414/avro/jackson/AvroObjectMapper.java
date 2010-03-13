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
package com.g414.avro.jackson;

import java.io.IOException;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.util.Utf8;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializerProvider;
import org.codehaus.jackson.map.ser.CustomSerializerFactory;

public class AvroObjectMapper {
	public static ObjectMapper getObjectMapper() {
		CustomSerializerFactory f = new CustomSerializerFactory();
		f.addSpecificMapping(Utf8.class, new Utf8Serializer());
		f.addSpecificMapping(Record.class, new RecordSerializer());

		ObjectMapper mapper = new ObjectMapper();
		mapper.setSerializerFactory(f);

		return mapper;
	}

	public static class Utf8Serializer extends JsonSerializer<Utf8> {
		@Override
		public void serialize(Utf8 value, JsonGenerator jgen,
				SerializerProvider provider) throws IOException,
				JsonProcessingException {
			jgen.writeString(value.toString());
		}
	}

	public static class RecordSerializer extends JsonSerializer<Record> {
		@Override
		public void serialize(Record record, JsonGenerator jgen,
				SerializerProvider provider) throws IOException,
				JsonProcessingException {
			jgen.writeStartObject();
			for (Field field : record.getSchema().getFields()) {
				String fieldName = field.name();

				Object value = record.get(fieldName);
				if (value != null && value instanceof Utf8) {
					jgen.writeObjectField(fieldName, value.toString());
				} else {
					jgen.writeObjectField(fieldName, value);
				}
			}
			jgen.writeEndObject();
		}
	}
}
