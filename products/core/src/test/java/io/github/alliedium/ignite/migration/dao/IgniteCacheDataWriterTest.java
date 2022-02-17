package io.github.alliedium.ignite.migration.dao;

import io.github.alliedium.ignite.migration.ClientIgniteBaseTest;
import io.github.alliedium.ignite.migration.dao.converters.BinaryObjectConverter;
import io.github.alliedium.ignite.migration.dao.converters.IIgniteDTOConverter;
import io.github.alliedium.ignite.migration.dao.converters.TypesResolver;
import io.github.alliedium.ignite.migration.dao.dataaccessor.IgniteCacheDAO;
import io.github.alliedium.ignite.migration.dao.datamanager.BinaryObjectFieldsInfoResolver;
import io.github.alliedium.ignite.migration.dao.datamanager.IBinaryObjectFieldInfoResolver;
import io.github.alliedium.ignite.migration.dto.*;
import io.github.alliedium.ignite.migration.dao.converters.IgniteObjectStringConverter;
import io.github.alliedium.ignite.migration.test.model.City;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.cache.Cache;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IgniteCacheDataWriterTest extends ClientIgniteBaseTest {

    private final String cacheName = "test_cache";
    private ICacheEntryValue cacheValueDTO;
    private IIgniteDTOConverter<String, Object> converter;
    private IgniteCacheDataWriter cacheDataWriter;
    private IgniteCache<Integer, City> cache;
    private CacheMetaData metaData;

    @BeforeMethod
    public void beforeIgniteCacheDataWriterTestMethod() {
        ignite.destroyCache(cacheName);
        CacheConfiguration<Integer, City> configuration = clientAPI.createTestCityCacheConfiguration(cacheName);
        cache = ignite.createCache(configuration);
        converter = IgniteObjectStringConverter.GENERIC_CONVERTER;
        cacheDataWriter = new IgniteCacheDataWriter(converter, ignite);
        metaData = mock(CacheMetaData.class);
        when(metaData.getTypes()).thenReturn(new CacheDataTypes(Integer.class.getName(), City.class.getName()));
        when(metaData.getCacheName()).thenReturn(cacheName);
        cacheDataWriter.getMetaDataConsumer().write(metaData);

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

        List<ICacheEntryValueField> cacheEntryValueDTOFields = new ArrayList<>();
        CacheEntryValueField field = new CacheEntryValueField.Builder()
                .setName("key")
                .setTypeClassName(TypesResolver.toAvroType(Integer.class.getName()))
                .setValue(cityIndex)
                .build();
        cacheEntryValueDTOFields.add(field);

        ICacheEntryValue cacheKeyDTO = new CacheEntryValue(cacheEntryValueDTOFields);

        return new CacheData(cacheName, cacheKeyDTO, cacheValueDTO);
    }

    private void prepareCacheEntryValue() {
        cache.put(0, new City("test_city", "test_district", random.nextInt()));

        IgniteCacheDAO igniteCacheDAO = new IgniteCacheDAO(ignite, cacheName);
        BinaryObject cacheBinaryObject = igniteCacheDAO.getAnyValue();
        IBinaryObjectFieldInfoResolver cacheFieldMetaBuilder = new BinaryObjectFieldsInfoResolver(cacheBinaryObject);
        IIgniteDTOConverter<ICacheEntryValue, BinaryObject> cacheValueConverter = new BinaryObjectConverter(cacheFieldMetaBuilder.resolveFieldsInfo());

        ScanQuery<Object, BinaryObject> scanQuery = new ScanQuery<>();
        for (Cache.Entry<Object, BinaryObject> entry : cache.withKeepBinary().query(scanQuery)) {
            cacheValueDTO = cacheValueConverter.convertFromEntity(entry.getValue());
            break;
        }
        cache.remove(0);
    }
}