package io.github.alliedium.ignite.migration.dto;

import io.github.alliedium.ignite.migration.Utils;

/**
 * Contains and returns configurations of a particular ignite cache.
 * Returning value is String, suitable for both serialization or creation of a new cache in ignite.
 * {@link org.apache.ignite.configuration.CacheConfiguration} object encapsulates a lot of Ignite specific data needed for further ignite cache recreation.
 * Therefore {@link org.apache.ignite.configuration.CacheConfiguration} for each existing ignite cache is being
 * serialized to String containing XML representation of serialized object.
 * Current unit is used be a String holder for storing an ignite cache configuration serialized to XML String.
 *
 * @see Utils#serializeObjectToXML(Object)
 */
public class CacheConfigurationData implements ICacheConfigurationData {

    private final String configuration;

    public CacheConfigurationData(String configuration) {
        this.configuration = configuration;
    }

    public String toString() {
        return this.configuration;
    }

}
