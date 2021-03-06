package io.github.alliedium.ignite.migration.dto;

import io.github.alliedium.ignite.migration.Utils;

/**
 * Contains and returns a key of Apache Ignite cache entry.
 * Returning value is String, suitable for both serialization or usage as a key when storing data into Apache Ignite cache.
 * Apache Ignite cache entry key is represented as an {@link org.apache.ignite.cache.affinity.AffinityKey} in the majority of cases.
 * Each Apache Ignite cache entry key is being serialized to String containing XML representation of serialized object in order to remain keeping ignite-specific key structure.
 * Current unit is used be a String holder for storing an Apache Ignite cache entry key serialized to XML String.
 *
 * @see Utils#serializeObjectToXML(Object)
 */
public class CacheEntryKey implements ICacheEntryKey {

    private final String key;

    public CacheEntryKey(String key) {
        this.key = key;
    }

    public String toString() {
        return this.key;
    }

}
