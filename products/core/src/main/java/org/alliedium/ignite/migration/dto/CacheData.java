package org.alliedium.ignite.migration.dto;

public class CacheData implements ICacheData {

    private final String cacheName;
    private final ICacheEntryValue cacheEntryKey;
    private final ICacheEntryValue cacheEntryValue;

    public CacheData(String cacheName, ICacheEntryValue cacheEntryKey, ICacheEntryValue cacheEntryValue) {
        this.cacheName = cacheName;
        this.cacheEntryKey = cacheEntryKey;
        this.cacheEntryValue = cacheEntryValue;
    }

    @Override
    public String getCacheName() {
        return cacheName;
    }

    @Override
    public ICacheEntryValue getCacheEntryKey() {
        return cacheEntryKey;
    }

    @Override
    public ICacheEntryValue getCacheEntryValue() {
        return cacheEntryValue;
    }
}
