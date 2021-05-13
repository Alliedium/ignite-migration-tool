package org.alliedium.ignite.migration.dto;

public class CacheMetaData implements ICacheMetaData {
    private final String cacheName;
    private final ICacheConfigurationData configuration;
    private final ICacheEntryMetaData entryMeta;

    public CacheMetaData(String cacheName, ICacheConfigurationData configuration, ICacheEntryMetaData entryMeta) {
        this.cacheName = cacheName;
        this.configuration = configuration;
        this.entryMeta = entryMeta;
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
}
