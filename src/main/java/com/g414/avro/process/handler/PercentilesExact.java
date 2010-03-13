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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;

import com.g414.avro.process.ProcessingException;
import com.g414.avro.process.RecordHandler;

/**
 * A RecordHandler instance that can be used to compute percentiles of a given
 * field for a collection of records. Called PercentilesExact because it
 * collects all values, sorts, and returns percentiles based on sorted input. In
 * the future, there may be other implementations that use sampling to conserve
 * memory rather than collecting all values.
 */
public class PercentilesExact<T extends Comparable<T>> implements RecordHandler {
    /** field to examine */
    protected final String field;

    /** whether sort should be by ascending or descending order */
    protected final boolean isAscending;

    /** values collected while processing */
    protected List<T> values = Collections.synchronizedList(new ArrayList<T>());

    /**
     * Construct a new instance that examines the given field and is ready to
     * return percentiles based on sorting ascending or descending.
     */
    public PercentilesExact(String field, boolean isAscending) {
        this.field = field;
        this.isAscending = isAscending;
    }

    /** @see RecordHandler#start() */
    @Override
    public void start() {
    }

    /** @see RecordHandler#handle(GenericRecord) */
    @Override
    @SuppressWarnings("unchecked")
    public void handle(GenericRecord record) throws ProcessingException {
        T value = (T) record.get(field);
        values.add(value);
    }

    /** @see RecordHandler#finish() */
    @Override
    public void finish() {
    }

    /**
     * Returns a map of BigDecimal percentiles to corresponding values seen in
     * input. Uses BigDecimals to avoid rounding error of desired percentiles.
     */
    public synchronized Map<BigDecimal, T> getPercentiles(
            List<BigDecimal> percentiles) {
        Collections.sort(values, new Comparator<T>() {
            public int compare(T o1, T o2) {
                int cmp = o1.compareTo(o2);
                return isAscending ? cmp : -cmp;
            };
        });

        BigDecimal size = new BigDecimal(values.size());

        Map<BigDecimal, T> outList = new LinkedHashMap<BigDecimal, T>();
        for (BigDecimal percentile : percentiles) {
            if (percentile.compareTo(BigDecimal.ZERO) < 0
                    || percentile.compareTo(BigDecimal.ONE) > 0) {
                throw new IllegalArgumentException(
                        "percentile must be between 0 and 1, inclusive: got "
                                + percentile);
            }

            int index = percentile.multiply(size).intValue();
            outList.put(percentile, values.get(index));
        }

        return Collections.unmodifiableMap(outList);
    }
}
