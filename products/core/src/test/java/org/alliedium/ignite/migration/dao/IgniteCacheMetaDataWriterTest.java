package org.alliedium.ignite.migration.dao;

import org.alliedium.ignite.migration.dao.converters.IgniteObjectStringConverter;
import org.alliedium.ignite.migration.dto.CacheConfigurationData;
import org.alliedium.ignite.migration.dto.CacheEntryMetaData;
import org.alliedium.ignite.migration.dto.CacheMetaData;
import org.apache.ignite.Ignite;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collection;
import java.util.Collections;

public class IgniteCacheMetaDataWriterTest {

    @Test
    public void write() {
        IgniteObjectStringConverter stringConverter = new IgniteObjectStringConverter();
        Ignite ignite = Mockito.mock(Ignite.class);
        CacheConfiguration<Object, BinaryObject> cacheConfiguration = new CacheConfiguration<>();
        Collection<QueryEntity> queryEntityCollection = Collections.singleton(new QueryEntity());
        IgniteCacheMetaDataWriter cacheMetaDataWriter = new IgniteCacheMetaDataWriter(stringConverter, ignite);

        CacheMetaData metaData = new CacheMetaData("cacheName",
                new CacheConfigurationData(stringConverter.convertFromEntity(cacheConfiguration)),
                new CacheEntryMetaData(stringConverter.convertFromEntity(queryEntityCollection)));

        cacheMetaDataWriter.write(metaData);

        Mockito.verify(ignite).getOrCreateCache(Mockito.any(CacheConfiguration.class));
    }
}