package io.github.alliedium.ignite.migration.dto;

public class CacheMetaData implements ICacheMetaData {
    private final String cacheName;
    private final ICacheConfigurationData configuration;
    private final ICacheEntryMetaData entryMeta;
    private final CacheDataTypes cacheDataTypes;

    public CacheMetaData(String cacheName, ICacheConfigurationData configuration, ICacheEntryMetaData entryMeta,
                         CacheDataTypes dataTypes) {
        this.cacheName = cacheName;
        this.configuration = configuration;
        this.entryMeta = entryMeta;
        this.cacheDataTypes = dataTypes;
    }

    @Override
    public String getCacheName() {
        return cacheName;
    }

    @Override
    public ICacheConfigurationData getConfiguration() {
        return configuration;
    }

    @Override
    public ICacheEntryMetaData getEntryMeta() {
        return entryMeta;
    }

    @Override
    public CacheDataTypes getTypes() {
        return cacheDataTypes;
    }
}
