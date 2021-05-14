package org.alliedium.ignite.migration.test;

import org.alliedium.ignite.migration.test.model.City;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class ClientAPI {
    private final TestDirectories testDirectories;
    private final Ignite ignite;
    private final Random random = new Random();

    private ClientAPI(IgniteConfiguration igniteConfiguration) {
        testDirectories = new TestDirectories();
        ignite = Ignition.getOrStart(igniteConfiguration);
    }

    public static ClientAPI loadClientIgnite(IgniteConfiguration igniteConfiguration) {
        return new ClientAPI(igniteConfiguration);
    }

    public Path getAvroTestSetPath() {
        return testDirectories.getAvroTestSetPath();
    }

    public Path getAvroMainPath() {
        return testDirectories.getAvroMainPath();
    }

    public Ignite getIgnite() {
        return ignite;
    }

    public Random getRandom() {
        return random;
    }

    public void cleanIgniteAndRemoveDirectories() throws IOException {
        ignite.cacheNames().forEach(ignite::destroyCache);
        deleteDirectoryRecursively(getAvroTestSetPath());
    }

    public void assertAtomicLongs(Map<String, Long> atomicLongs) {
        atomicLongs.forEach((name, val) -> {
            IgniteAtomicLong atomicLong = ignite.atomicLong(name, 0, false);
            Objects.requireNonNull(atomicLong);
            assertEquals(val, atomicLong.get());
        });
    }

    public void closeAtomicLongs(Map<String, Long> atomicLongs) {
        atomicLongs.forEach((name, val) -> ignite.atomicLong(name, val, false).close());
    }

    public static void deleteDirectoryRecursively(Path directoryPath) throws IOException {
        if (Files.exists(directoryPath)) {
            Files.walk(directoryPath)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
    }

    public QueryEntity createQueryEntityForCity() {
        return new QueryEntity()
                .setValueType(City.class.getName())
                .setKeyType(Integer.class.getName())
                .addQueryField("name", String.class.getName(), null)
                .addQueryField("district", String.class.getName(), null)
                .addQueryField("population", Integer.class.getName(), null);
    }

    public CacheConfiguration<Integer, City> createTestCityCacheConfiguration(String cacheName) {
        CacheConfiguration<Integer, City> cacheConfiguration = new CacheConfiguration<>();
        cacheConfiguration.setQueryEntities(Collections.singleton(createQueryEntityForCity()));
        cacheConfiguration.setName(cacheName);
        return cacheConfiguration;
    }

    public List<City> createTestCityCacheAndInsertData(String cacheName, int count) {
        CacheConfiguration<Integer, City> cacheConfiguration = createTestCityCacheConfiguration(cacheName);
        IgniteCache<Integer, City> igniteCache = ignite.createCache(cacheConfiguration);
        List<City> testCities = new ArrayList<>();
        for (int cityIndex = 0; cityIndex < count; cityIndex++) {
            testCities.add(cityIndex, new City("test_city" + cityIndex, "test_district", random.nextInt()));
            igniteCache.put(cityIndex, testCities.get(cityIndex));
        }

        return testCities;
    }

    public void clearIgniteAndCheckIgniteIsEmpty() {
        clearIgniteAndCheckIgniteIsEmpty(new HashMap<>());
    }

    public void clearIgniteAndCheckIgniteIsEmpty(Map<String, Long> atomicLongs) {
        // # ignite test data deletion
        Collection<String> cacheNames = ignite.cacheNames();
        cacheNames.forEach(ignite::destroyCache);
        closeAtomicLongs(atomicLongs);

        // # assertion that no data is present in ignite
        cacheNames.forEach(cacheName -> assertNull(ignite.cache(cacheName)));
        atomicLongs.forEach((name, val) -> assertNull(ignite.atomicLong(name, val, false)));
    }

    public <T> void assertIgniteCacheEqualsList(List<T> list, String cacheName){
        IgniteCache<Integer, T> igniteCache = ignite.cache(cacheName);
        for (int itemIndex = 0; itemIndex < list.size(); itemIndex++) {
            assertEquals(list.get(itemIndex), igniteCache.get(itemIndex));
        }
    }

    private void assertNull(Object obj) {
        if (!Objects.isNull(obj)) {
            throw new AssertionError("provided object is not null, obj: " + obj);
        }
    }

    private <T> void assertEquals(T expected, T actual) {
        if (!expected.equals(actual)) {
            throw new AssertionError(
                    String.format("Expected and actual are not equal,\nexpected: %s\nactual: %s", expected, actual));
        }
    }
}
