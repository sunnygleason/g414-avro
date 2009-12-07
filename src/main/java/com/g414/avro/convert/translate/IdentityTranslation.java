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
		Record record = new Record(schema);
		record.putAll(in);

		return record;
	}
}
