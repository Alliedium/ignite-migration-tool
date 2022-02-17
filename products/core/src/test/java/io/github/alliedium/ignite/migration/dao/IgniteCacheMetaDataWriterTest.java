package io.github.alliedium.ignite.migration.dao;

import io.github.alliedium.ignite.migration.dto.CacheDataTypes;
import io.github.alliedium.ignite.migration.dao.converters.IgniteObjectStringConverter;
import io.github.alliedium.ignite.migration.dto.CacheConfigurationData;
import io.github.alliedium.ignite.migration.dto.CacheEntryMetaData;
import io.github.alliedium.ignite.migration.dto.CacheMetaData;
import org.apache.ignite.Ignite;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.testng.annotations.Test;
import org.mockito.Mockito;

import java.util.Collection;
import java.util.Collections;

public class IgniteCacheMetaDataWriterTest {

    @Test
    public void write() {
        Ignite ignite = Mockito.mock(Ignite.class);
        CacheConfiguration<Object, BinaryObject> cacheConfiguration = new CacheConfiguration<>();
        Collection<QueryEntity> queryEntityCollection = Collections.singleton(new QueryEntity());
        IgniteCacheMetaDataWriter cacheMetaDataWriter = new IgniteCacheMetaDataWriter(IgniteObjectStringConverter.GENERIC_CONVERTER, ignite);

        CacheMetaData metaData = new CacheMetaData("cacheName",
                new CacheConfigurationData(IgniteObjectStringConverter.CACHE_CONFIG_CONVERTER.convertFromEntity(cacheConfiguration)),
                new CacheEntryMetaData(IgniteObjectStringConverter.QUERY_ENTITY_CONVERTER.convertFromEntity(queryEntityCollection)),
                new CacheDataTypes(String.class.getName(), String.class.getName()));

        cacheMetaDataWriter.write(metaData);

        Mockito.verify(ignite).getOrCreateCache(Mockito.any(CacheConfiguration.class));
    }
}