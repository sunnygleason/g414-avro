package com.g414.avro.collect;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An Object tracker that creates a frequency ordering which may be used for
 * item-based compression. Not thread-safe.
 */
public class FrequencyTracker<T> {
	/** collection mapping keys to integer counts */
	private final Map<T, Integer> counts = new HashMap<T, Integer>();

	/**
	 * Increment the count for a given key object.
	 */
	public void increment(T key) {
		Integer value = counts.get(key);

		if (value == null) {
			value = Integer.valueOf(0);
		}

		counts.put(key, Integer.valueOf(value.intValue() + 1));
	}

	/**
	 * Return the list of keys in descending order of frequency.
	 */
	public List<T> getKeys() {
		return Collections.unmodifiableList(getKeysImpl());
	}

	/**
	 * Return the top N keys in descending order of frequency.
	 */
	public List<T> getTopNKeys(int n) {
		List<T> outKeys = getKeysImpl();

		if (n < outKeys.size()) {
			outKeys.subList(n, outKeys.size() - 1).clear();
		}

		return Collections.unmodifiableList(outKeys);
	}

	/**
	 * Returns a copy of the frequency map.
	 */
	public Map<T, Integer> getFrequencies() {
		return Collections.unmodifiableMap(counts);
	}

	/**
	 * Returns a modifiable list of keys in descending order of frequency.
	 */
	private List<T> getKeysImpl() {
		List<T> outKeys = new ArrayList<T>(counts.size());
		outKeys.addAll(counts.keySet());

		Collections.sort(outKeys, new Comparator<T>() {
			public int compare(T o1, T o2) {
				return -1 * counts.get(o1).compareTo(counts.get(o2));
			};
		});

		return outKeys;
	}
}
