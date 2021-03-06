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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData.Record;

import com.g414.avro.convert.RecordTranslation;

/**
 * A RecordTranslation that does not modify records, just copies them.
 */
public class IdentityTranslation implements RecordTranslation {
    /** schema to use */
    protected final Schema schema;

    /** Constructs a new instance using the specified schema */
    public IdentityTranslation(Schema schema) {
        this.schema = schema;
    }

    /** @see RecordTranslation#translate() */
    @Override
    public GenericRecord translate(GenericRecord in) {
        int size = in.getSchema().getFields().size();

        Record record = new Record(schema);

        for (int i = 0; i < size; i++) {
            record.put(i, in.get(i));
        }

        return record;
    }
}
