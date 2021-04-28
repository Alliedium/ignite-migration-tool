package org.alliedium.ignite.migration.dao;

import org.alliedium.ignite.migration.ClientIgniteBaseTest;
import org.alliedium.ignite.migration.dao.converters.IIgniteDTOConverter;
import org.alliedium.ignite.migration.dao.converters.IgniteBinaryObjectConverter;
import org.alliedium.ignite.migration.dao.converters.IgniteObjectStringConverter;
import org.alliedium.ignite.migration.dao.dataaccessor.IgniteCacheDAO;
import org.alliedium.ignite.migration.dao.datamanager.IIgniteCacheFieldMetaBuilder;
import org.alliedium.ignite.migration.dao.datamanager.IgniteCacheFieldMetaBuilder;
import org.alliedium.ignite.migration.dao.dtobuilder.CacheKeyBuilder;
import org.alliedium.ignite.migration.dto.CacheData;
import org.alliedium.ignite.migration.dto.ICacheData;
import org.alliedium.ignite.migration.dto.ICacheEntryKey;
import org.alliedium.ignite.migration.dto.ICacheEntryValue;
import org.alliedium.ignite.migration.test.model.City;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.junit.Assert;
import org.junit.Test;

import javax.cache.Cache;

public class IgniteCacheDataWriterTest extends ClientIgniteBaseTest {

    private final String cacheName = "test_cache";
    private final IIgniteDTOConverter<String, Object> cacheKeyConverter = new IgniteObjectStringConverter();
    private ICacheEntryValue cacheValueDTO;

    @Test
    public void write() {
        CacheConfiguration<Integer, City> configuration = clientAPI.createTestCityCacheConfiguration(cacheName);
        ignite.createCache(configuration);

        ICacheData cacheData = getNextCacheData();

        IgniteObjectStringConverter converter = new IgniteObjectStringConverter();
        IgniteCacheDataWriter cacheDataWriter = new IgniteCacheDataWriter(converter, ignite);

        cacheDataWriter.write(cacheData);

        Assert.assertNotNull(ignite.cache(cacheName).get(1));
    }

    @Test
    public void testMultipleWrite() {
        CacheConfiguration<Integer, City> configuration = clientAPI.createTestCityCacheConfiguration(cacheName);
        ignite.createCache(configuration);
        IgniteObjectStringConverter converter = new IgniteObjectStringConverter();
        IgniteCacheDataWriter cacheDataWriter = new IgniteCacheDataWriter(converter, ignite);

        for (int i = 1; i < 1_000; i++) {
            ICacheData cacheData = getNextCacheData();
            cacheDataWriter.write(cacheData);
            Assert.assertNotNull(ignite.cache(cacheName).get(i));
        }

        cacheDataWriter.close();
    }

    private ICacheData getNextCacheData() {
        if (cacheValueDTO == null) {
            ignite.cache(cacheName).put(0, new City("test_city", "test_district", random.nextInt()));

            IgniteCacheDAO igniteCacheDAO = new IgniteCacheDAO(ignite, cacheName);
            BinaryObject cacheBinaryObject = igniteCacheDAO.getAnyValue();
            IIgniteCacheFieldMetaBuilder cacheFieldMetaBuilder = new IgniteCacheFieldMetaBuilder(cacheBinaryObject, igniteCacheDAO.getCacheQueryEntities());
            IIgniteDTOConverter<ICacheEntryValue, BinaryObject> cacheValueConverter = new IgniteBinaryObjectConverter(cacheFieldMetaBuilder.getFieldsMetaData());


            ScanQuery<Object, BinaryObject> scanQuery = new ScanQuery<>();

            for (Cache.Entry<Object, BinaryObject> entry : ignite.cache(cacheName).withKeepBinary().query(scanQuery)) {
                cacheValueDTO = cacheValueConverter.convertFromEntity(entry.getValue());
                break;
            }
        }

        ICacheEntryKey cacheKeyDTO = new CacheKeyBuilder(ignite.cache(cacheName).size(), cacheKeyConverter).build();

        return new CacheData(cacheName, cacheKeyDTO, cacheValueDTO);
    }
}