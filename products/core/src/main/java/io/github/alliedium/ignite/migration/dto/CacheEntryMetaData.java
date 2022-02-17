package io.github.alliedium.ignite.migration.dto;

import io.github.alliedium.ignite.migration.Utils;

/**
 * Contains and returns query entities of a particular Apache Ignite cache.
 * Returning value is String, suitable for both serialization or including into Apache Ignite cache configurations.
 * {@link org.apache.ignite.cache.QueryEntity} object encapsulates a lot of Apache Ignite specific data needed for further Apache Ignite cache configuring.
 * Therefore the collection of {@link org.apache.ignite.cache.QueryEntity} objects for each existing Apache Ignite cache is being
 * serialized to String containing XML representation of serialized object.
 * Current unit is used be a String holder for storing an Apache Ignite cache query entities serialized to XML String.
 *
 * @see Utils#serializeObjectToXML(Object)
 */
public class CacheEntryMetaData implements ICacheEntryMetaData {

    private final String cacheEntryMeta;

    public CacheEntryMetaData(String cacheEntryMeta) {
        this.cacheEntryMeta = cacheEntryMeta;
    }

    public String toString() {
        return this.cacheEntryMeta;
    }

}
