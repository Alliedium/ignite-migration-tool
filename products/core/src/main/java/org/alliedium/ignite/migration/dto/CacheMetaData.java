package org.alliedium.ignite.migration.dto;

public class CacheMetaData implements ICacheMetaData {
    private final String name;
    private final ICacheConfigurationData configuration;
    private final ICacheEntryMetaData entryMeta;

    public CacheMetaData(String name, ICacheConfigurationData configuration, ICacheEntryMetaData entryMeta) {
        this.name = name;
        this.configuration = configuration;
        this.entryMeta = entryMeta;
    }

    @Override
    public String getName() {
        return name;
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
