package org.alliedium.ignite.migration;

import org.alliedium.ignite.migration.test.model.IdContainer;
import org.alliedium.ignite.migration.test.model.Passport;
import org.alliedium.ignite.migration.test.model.Person;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;

import javax.cache.Cache;
import java.io.IOException;
import java.util.*;

public class TestNestedObjects extends ClientIgniteBaseTest {

    @Test
    public void testNested() throws IOException {
        String cacheName = "personCache";
        Ignite ignite = clientAPI.getIgnite();
        CacheConfiguration<AffinityKey<Integer>, Person> cacheConfiguration = new CacheConfiguration<>();
        QueryEntity queryEntity = new QueryEntity()
                .setValueType(Person.class.getName())
                .setKeyType(AffinityKey.class.getName())
                .addQueryField("name", String.class.getName(), null)
                .addQueryField("age", Integer.class.getName(), null)
                .addQueryField("passport", Passport.class.getName(), null);
        cacheConfiguration.setQueryEntities(Collections.singletonList(queryEntity));
        cacheConfiguration.setName(cacheName);

        IgniteCache<AffinityKey<Integer>, Person> cache = ignite.createCache(cacheConfiguration);
        Map<AffinityKey<Integer>, Person> map = new HashMap<>();

        for (int i = 0; i < 10; i++) {
            Person person = new Person("testPerson:" + i,
                    new Passport(UUID.randomUUID().toString()), 20 + i);
            AffinityKey<Integer> affinityKey = new AffinityKey<>(
                    i, new IdContainer(person.getPassport().getId()));
            cache.put(affinityKey, person);
            map.put(affinityKey, person);
        }

        Controller controller = new Controller(ignite, IgniteAtomicLongNamesProvider.EMPTY);
        controller.serializeDataToAvro(clientAPI.getAvroTestSetPath());

        clientAPI.clearIgniteAndCheckIgniteIsEmpty();

        controller.deserializeDataFromAvro(clientAPI.getAvroTestSetPath());

        IgniteCache<AffinityKey<Integer>, Person> recreatedCache = clientAPI.getIgnite().cache(cacheName);

        List<Cache.Entry<AffinityKey<Integer>, Person>> values = recreatedCache.query(new ScanQuery<AffinityKey<Integer>, Person>())
                .getAll();

        map.forEach((key, val) -> {
            boolean hasEntry = values.stream().anyMatch(entry ->
                        key.equals(entry.getKey()) && val.equals(entry.getValue()));
            Assert.assertTrue(hasEntry);
        });

        clientAPI.cleanIgniteAndRemoveDirectories();
    }
}
