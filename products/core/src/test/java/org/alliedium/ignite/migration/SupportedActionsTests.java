package org.alliedium.ignite.migration;
import org.alliedium.ignite.migration.dao.dataaccessor.IgniteAtomicLongNamesProvider;
import org.alliedium.ignite.migration.test.model.City;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class SupportedActionsTests extends ClientIgniteBaseTest {

    /**
     * This test shows that data in cache will be overridden in case a user will try to deserialize
     * cache which already exists in ignite.
     * The Deserialize operation will override values by keys, no other objects will be touched.
     * For example if a cache contains the following data:
     *      [first : firstObj, second : secondObj]
     * And serialized data contains the following data:
     *      [first : thirdObj]
     * The result of deserialize operation will be the following:
     *      [first : thirdObj, second : secondObj]
     */
    @Test
    public void dirtyWriteToIgniteTest() {
        String cacheName = "testCache";
        clientAPI.createTestCityCacheAndInsertData(cacheName, 100);

        Controller controller = new Controller(clientAPI.getIgnite(), IgniteAtomicLongNamesProvider.EMPTY);
        controller.serializeDataToAvro(clientAPI.getAvroTestSetPath());

        IgniteCache<Integer, City> cache = clientAPI.getIgnite().cache(cacheName);
        for(int cityIndex = 0; cityIndex < 50; cityIndex++) {
            cache.remove(cityIndex);
        }
        City city = new City("1", "1", 1);
        cache.put(50, city);

        controller.deserializeDataFromAvro(clientAPI.getAvroTestSetPath());

        Assert.assertEquals(100, cache.size());
        Assert.assertNotEquals(city, cache.get(50));

        cache.put(-1, city);

        Assert.assertEquals(101, cache.size());

        controller.deserializeDataFromAvro(clientAPI.getAvroTestSetPath());

        Assert.assertEquals(101, cache.size());
        Assert.assertEquals(city, cache.get(-1));
    }

    /**
     * This test shows that Apache Ignite migration tool works in the same way
     * with both: in memory caches and persisted caches.
     */
    @Test
    public void inmemoryCachesAreSerializedTest() {
        String cacheName = "inmemoryCachesAreSerializedTest";
        String dataRegionName = "InMemory";
        CacheConfiguration<Integer, City> configuration = clientAPI.createTestCityCacheConfiguration(cacheName);
        configuration.setDataRegionName(dataRegionName);

        List<City> cities = createCacheAndFillWithData(configuration,
                () -> new City("testCity", "testDistrict", 0), 10);

        Controller controller = new Controller(ignite, IgniteAtomicLongNamesProvider.EMPTY);
        controller.serializeDataToAvro(clientAPI.getAvroTestSetPath());

        clientAPI.clearIgniteAndCheckIgniteIsEmpty();

        controller.deserializeDataFromAvro(clientAPI.getAvroTestSetPath());

        clientAPI.assertIgniteCacheEqualsList(cities, cacheName);
    }
}
