package com.g414.avro.convert;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.g414.avro.process.ProcessingException;
import com.g414.avro.process.RecordFilter;
import com.g414.avro.process.RecordHandler;
import com.g414.avro.process.RecordProcessor;

/**
 * A RecordProcessor that uses a given RecordTranslation to convert records from
 * a given reader and write them to a specified record handler.
 */
public class RecordConverter extends RecordProcessor<GenericRecord> {
	/**
	 * Create a new RecordProcessor that reads records of the specified schema
	 * matching the given filter, applies the given translation of those records
	 * to new records which are sent to the specified output handler.
	 */
	public RecordConverter(Schema inschema, final RecordFilter infilter,
			final RecordTranslation translation, final RecordHandler outhandler) {
		super(inschema, infilter, new RecordHandler() {
			/** @see RecordHandler#start() */
			@Override
			public void start() {
				outhandler.start();
			}

			/** @see RecordHandler#handle(GenericRecord) */
			@Override
			public void handle(GenericRecord record) throws ProcessingException {
				GenericRecord outrecord = translation.translate(record);
				outhandler.handle(outrecord);
			}

			/** @see RecordHandler#finish() */
			@Override
			public void finish() {
				outhandler.finish();
			}
		});
	}
}
