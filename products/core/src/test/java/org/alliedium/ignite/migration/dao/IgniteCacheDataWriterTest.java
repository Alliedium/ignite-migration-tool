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
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.asserts.Assertion;
import org.testng.asserts.IAssert;

import javax.cache.Cache;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class IgniteCacheDataWriterTest extends ClientIgniteBaseTest {

    private final String cacheName = "test_cache";
    private ICacheEntryValue cacheValueDTO;
    private IgniteObjectStringConverter converter;
    private IgniteCacheDataWriter cacheDataWriter;
    private IgniteCache<Integer, City> cache;

    @BeforeMethod
    public void beforeIgniteCacheDataWriterTestMethod() {
        ignite.destroyCache(cacheName);
        CacheConfiguration<Integer, City> configuration = clientAPI.createTestCityCacheConfiguration(cacheName);
        cache = ignite.createCache(configuration);
        converter = new IgniteObjectStringConverter();
        cacheDataWriter = new IgniteCacheDataWriter(converter, ignite);
    }

    @AfterMethod
    public void afterIgniteCacheDataWriterTestMethod() {
        ignite.destroyCache(cacheName);
    }

    @Test
    public void write() {
        ICacheData cacheData = getNextCacheData(0);

        cacheDataWriter.write(cacheData);
        cacheDataWriter.close();

        Assert.assertTrue(cache.size() > 0);
    }

    @Test
    public void testMultipleWrite() {
        for (int cityIndex = 0; cityIndex < 1_000; cityIndex++) {
            ICacheData cacheData = getNextCacheData(cityIndex);
            cacheDataWriter.write(cacheData);
        }
        cacheDataWriter.close();

        Assert.assertEquals(cache.size(), 1_000);
    }

    private ICacheData getNextCacheData(int cityIndex) {
        if (cacheValueDTO == null) {
            prepareCacheEntryValue();
        }

        ICacheEntryKey cacheKeyDTO = new CacheKeyBuilder(cityIndex, converter).build();
        return new CacheData(cacheName, cacheKeyDTO, cacheValueDTO);
    }

    private void prepareCacheEntryValue() {
        cache.put(0, new City("test_city", "test_district", random.nextInt()));

        IgniteCacheDAO igniteCacheDAO = new IgniteCacheDAO(ignite, cacheName);
        BinaryObject cacheBinaryObject = igniteCacheDAO.getAnyValue();
        IIgniteCacheFieldMetaBuilder cacheFieldMetaBuilder = new IgniteCacheFieldMetaBuilder(cacheBinaryObject, igniteCacheDAO.getCacheQueryEntities());
        IIgniteDTOConverter<ICacheEntryValue, BinaryObject> cacheValueConverter = new IgniteBinaryObjectConverter(cacheFieldMetaBuilder.getFieldsMetaData());

        ScanQuery<Object, BinaryObject> scanQuery = new ScanQuery<>();
        for (Cache.Entry<Object, BinaryObject> entry : cache.withKeepBinary().query(scanQuery)) {
            cacheValueDTO = cacheValueConverter.convertFromEntity(entry.getValue());
            break;
        }
        cache.remove(0);
    }
}