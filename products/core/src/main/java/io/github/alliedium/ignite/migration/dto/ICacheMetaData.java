package io.github.alliedium.ignite.migration.dto;

/**
 * Represents meta data from an ignite cache.
 * Returned data has no references to Ignite specific classes. It is suitable for either ignite cache recreation and repopulating
 * or meta-data serializing to an external format.
 */
public interface ICacheMetaData {
    String getCacheName();

    ICacheConfigurationData getConfiguration();

    ICacheEntryMetaData getEntryMeta();

    CacheDataTypes getTypes();
}
