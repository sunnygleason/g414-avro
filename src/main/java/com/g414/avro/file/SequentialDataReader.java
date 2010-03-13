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
package com.g414.avro.file;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;

import com.g414.avro.process.ProcessingException;

/**
 * Reads data files in a sequential manner, given a specified Schema. Since the
 * schema is presumed, this allows processing of compressed or otherwise encoded
 * streams.
 * 
 * This class is a derivative work of the org.apache.avro.file.DataFileReader
 * class that has been modified to have a lastPos() method and to not require a
 * SeekableInput.
 * 
 * @see org.apache.avro.file.DataFileReader
 */
public class SequentialDataReader<D> implements Tell {
    protected DatumReader<D> reader;
    protected PositionFilter in;
    protected Decoder vin;

    protected long lastPos = 0;
    protected long blockCount; // # entries in block
    protected byte[] sync;
    protected byte[] syncBuffer = new byte[DataFileConstants.SYNC_SIZE];

    /** Construct a reader for a file. */
    public SequentialDataReader(Schema schema, InputStream sin,
            DatumReader<D> reader) throws ProcessingException {
        try {
            this.in = new PositionFilter(sin);
            byte[] magic = new byte[4];
            in.read(magic);
            if (!Arrays.equals(DataFileConstants.MAGIC, magic))
                throw new IOException("Not a data file.");

            this.reader = reader;
            reader.setSchema(schema);

            this.vin = new BinaryDecoder(in);
        } catch (IOException e) {
            throw new ProcessingException("Exception while creating reader: "
                    + e.getMessage(), e);
        }
    }

    /** Return the next datum in the file. */
    public synchronized D next(D reuse) throws ProcessingException {
        try {
            while (blockCount == 0) { // at start of block
                skipSync(); // skip a sync
                blockCount = vin.readLong(); // read blockCount
                if (blockCount == DataFileConstants.FOOTER_BLOCK) {
                    return null;
                }
            }
            blockCount--;
            lastPos = in.tell();
            return reader.read(reuse, vin);
        } catch (IOException e) {
            throw new ProcessingException("Exception while reading input: "
                    + e.getMessage(), e);
        }
    }

    /** return the start position of the last record returned */
    public synchronized long lastPos() {
        return this.lastPos;
    }

    /** Close this reader. */
    public synchronized void close() throws IOException {
        in.close();
    }

    /** skips a synchronization block */
    protected void skipSync() throws IOException {
        vin.readFixed(syncBuffer);
        if (sync == null) {
            sync = new byte[DataFileConstants.SYNC_SIZE];
            System.arraycopy(syncBuffer, 0, sync, 0, syncBuffer.length);
        }

        if (!Arrays.equals(syncBuffer, sync)) {
            throw new RuntimeException("Invalid Sync!");
        }
    }

    /** Utility class for implementing Tell */
    protected static class PositionFilter extends InputStream {
        private InputStream in;
        private long position;

        public PositionFilter(InputStream in) throws IOException {
            this.in = in;
        }

        public int read() throws IOException {
            int value = in.read();
            if (value != -1) {
                position += 1;
            }

            return value;
        }

        public int read(byte[] b, int off, int len) throws IOException {
            int value = in.read(b, off, len);
            if (value > 0) {
                position += value;
            }
            return value;
        }

        public long tell() {
            return this.position;
        }
    }
}
