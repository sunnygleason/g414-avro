package com.g414.avro.collect;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tracks a collection of items, converting keys to integers as a mechanism for
 * simple table-based compression. Not thread-safe.
 */
public class SequentialTracker<T> {
	/** collection mapping keys to integer indexes */
	private final Map<T, Integer> values = new LinkedHashMap<T, Integer>();

	/** holder for the next integer index */
	private AtomicInteger next = new AtomicInteger();

	/**
	 * Place a new key into the collection.
	 */
	public void put(T value) {
		if (values.containsKey(value)) {
			return;
		}

		values.put(value, next.getAndIncrement());
	}

	/**
	 * Obtain a value from the collection: the integer index if present,
	 * otherwise the value itself.
	 */
	public Object getIfPresent(T value) {
		if (values.containsKey(value)) {
			return values.get(value);
		}

		return value;
	}

	/**
	 * Obtain the index of the specified key, adding it to the collection if not
	 * present.
	 */
	public Integer getIndex(T value) {
		if (!values.containsKey(value)) {
			put(value);
		}

		return values.get(value);
	}

	/**
	 * Obtain the list of all Keys in order.
	 */
	public List<T> getKeys() {
		List<T> newList = new ArrayList<T>();

		for (T key : values.keySet()) {
			newList.add(key);
		}

		return Collections.unmodifiableList(newList);
	}
}
