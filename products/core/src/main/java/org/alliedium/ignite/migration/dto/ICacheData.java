package org.alliedium.ignite.migration.dto;

public interface ICacheData {
    String getCacheName();
    ICacheEntryValue getCacheEntryKey();
    ICacheEntryValue getCacheEntryValue();
}
