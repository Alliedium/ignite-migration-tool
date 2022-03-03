package io.github.alliedium.ignite.migration.patchtools;

import io.github.alliedium.ignite.migration.test.model.*;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SelectActionTest extends BaseTest {

    @Test
    public void selectTest(Method method) {
        String cacheName = method.getName();
        createCityCache(cacheName);
        controller.serializeDataToAvro(source.getPath());

        PatchContext context = new PatchContext(source, destination);
        context.prepare();

        context.patchCachesWhichEndWith(cacheName, cachePath -> {
            TransformAction<TransformOutput> action = new SelectAction.Builder()
                    .context(context)
                    .fields("key", "name", "district")
                    .from(cachePath)
                    .build();

            new CacheWriter(action).writeTo(destination.plus(cacheName).getPath().toString());
        });

        context.getPipeline().run().waitUntilFinish();

        controller.deserializeDataFromAvro(destination.getPath());

        clientAPI.getIgnite().cache(cacheName).withKeepBinary()
                .query(new ScanQuery<Object, BinaryObject>())
                .forEach(entry -> {
                    Assert.assertFalse(entry.getValue().hasField("population"));
                });
    }

    @Test
    public void testSelectNestedObjects(Method method) {
        String cacheName = method.toString();
        CacheConfiguration<AffinityKey<Integer>, Person> cacheConfiguration = new CacheConfiguration<>();
        cacheConfiguration.setName(cacheName);

        IgniteCache<AffinityKey<Integer>, Person> cache = ignite.createCache(cacheConfiguration);

        Person person = new Person("person1", new Passport("helloID"), 21);
        cache.put(new AffinityKey<>(1, new IdContainer(person.getPassport().getId())), person);

        controller.serializeDataToAvro(source.getPath());

        clientAPI.clearIgniteAndCheckIgniteIsEmpty();

        PatchContext context = new PatchContext(source, destination);
        context.prepare();

        context.patchCachesWhichEndWith(cacheName, cachePath -> {
            TransformAction<TransformOutput> action = new SelectAction.Builder()
                    .context(context)
                    .fields("key", "name", "passport")
                    .from(cachePath)
                    .build();

            new CacheWriter(action).writeTo(destination.plus(cacheName).getPath().toString());
        });

        context.getPipeline().run().waitUntilFinish();

        controller.deserializeDataFromAvro(destination.getPath());

        clientAPI.getIgnite().cache(cacheName).withKeepBinary()
                .query(new ScanQuery<Object, BinaryObject>())
                .forEach(entry -> {
                    Assert.assertFalse(entry.getValue().hasField("age"));
                });
    }

    @Test
    public void testSelectNestedObjectsListMap(Method method) {
        String cacheName = method.toString();
        CacheConfiguration<Integer, Flight> cacheConfiguration = new CacheConfiguration<>();
        cacheConfiguration.setName(cacheName);
        IgniteCache<Integer, Flight> cache = ignite.createCache(cacheConfiguration);

        Flight flight = FlightFactory.create();

        cache.put(1, flight);

        controller.serializeDataToAvro(source.getPath());

        clientAPI.clearIgniteAndCheckIgniteIsEmpty();

        PatchContext context = new PatchContext(source, destination);
        context.prepare();

        context.patchCachesWhichEndWith(cacheName, cachePath -> {
            TransformAction<TransformOutput> action = new SelectAction.Builder()
                    .context(context)
                    .fields("key", "id", "personList", "tickets")
                    .from(cachePath)
                    .build();

            new CacheWriter(action).writeTo(destination.plus(cacheName).getPath().toString());
        });

        context.getPipeline().run().waitUntilFinish();

        controller.deserializeDataFromAvro(destination.getPath());

        IgniteCache<Integer, Flight> flightsCache = clientAPI.getIgnite().cache(cacheName);
        Flight resultFlight = flightsCache.get(1);
        Assert.assertEquals(resultFlight, flight);
    }
}
