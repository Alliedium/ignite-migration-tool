package org.alliedium.ignite.migration.dto;

import org.alliedium.ignite.migration.Utils;

/**
 * Contains and returns query entities of a particular ignite cache.
 * Returning value is String, suitable for both serialization or including into ignite cache configurations.
 * {@link org.apache.ignite.cache.QueryEntity} object encapsulates a lot of Ignite specific data needed for further ignite cache configuring.
 * Therefore the collection of {@link org.apache.ignite.cache.QueryEntity} objects for each existing ignite cache is being
 * serialized to String containing XML representation of serialized object.
 * Current unit is used be a String holder for storing an ignite cache query entities serialized to XML String.
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
