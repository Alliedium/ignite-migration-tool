package org.alliedium.ignite.migration.dto;

public interface ICacheData {
    String getCacheName();
    ICacheEntryKey getCacheEntryKey();
    ICacheEntryValue getCacheEntryValue();
}
