package io.github.alliedium.ignite.migration.patchtools;

import io.github.alliedium.ignite.migration.test.model.IdContainer;
import io.github.alliedium.ignite.migration.test.model.Passport;
import io.github.alliedium.ignite.migration.test.model.Person;
import org.apache.beam.sdk.values.Row;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

public class MapActionTest extends BaseTest {

    @Test
    public void mapTest(Method method) {
        String cacheName = method.getName();
        createCityCache(cacheName);
        controller.serializeDataToAvro(source.getPath());

        PatchContext context = new PatchContext(source, destination);
        context.prepare();

        int populationValue = clientAPI.getRandom().nextInt();

        context.patchCachesWhichEndWith(cacheName, cachePath -> {
            TransformAction<TransformOutput> action = new SelectAction.Builder()
                    .context(context)
                    .fields("key", "name", "district", "population")
                    .from(cachePath)
                    .build();

            action = new MapAction.Builder()
                    .action(action)
                    .map(row -> Row.fromRow(row)
                            .withFieldValue("population", populationValue)
                            .build())
                    .build();

            new CacheWriter(action).writeTo(destination.plus(cacheName).getPath().toString());
        });

        context.getPipeline().run().waitUntilFinish();

        controller.deserializeDataFromAvro(destination.getPath());
        clientAPI.getIgnite().cache(cacheName).withKeepBinary()
                .query(new ScanQuery<Object, BinaryObject>())
                .forEach(entry -> {
                    Assert.assertTrue(entry.getValue().hasField("population"));
                    int population = entry.getValue().field("population");
                    Assert.assertEquals(populationValue, population);
                });
    }

    @Test
    public void testChangeNestedObjects(Method method) {
        String cacheName = method.toString();
        CacheConfiguration<AffinityKey<Integer>, Person> cacheConfiguration = new CacheConfiguration<>();
        cacheConfiguration.setName(cacheName);

        IgniteCache<AffinityKey<Integer>, Person> cache = ignite.createCache(cacheConfiguration);

        String passportId = "helloID";
        String passportIdChanged = "helloIDChanged";

        Person person = new Person("person1", new Passport(passportId), 21);
        cache.put(new AffinityKey<>(1, new IdContainer(person.getPassport().getId())), person);

        controller.serializeDataToAvro(source.getPath());

        clientAPI.clearIgniteAndCheckIgniteIsEmpty();

        PatchContext context = new PatchContext(source, destination);
        context.prepare();

        context.patchCachesWhichEndWith(cacheName, cachePath -> {
            TransformAction<TransformOutput> action = new SelectAction.Builder()
                    .context(context)
                    .fields("key", "name", "passport", "age")
                    .from(cachePath)
                    .build();
            action = new MapAction.Builder()
                    .action(action)
                    .map(row -> {
                        Row passport = row.getValue("passport");
                        Assert.assertNotNull(passport);
                        return Row.fromRow(row)
                                .withFieldValue("passport",
                                        Row.fromRow(passport)
                                                .withFieldValue("id", passportIdChanged)
                                                .build())
                                .build();
                    })
                    .build();

            new CacheWriter(action).writeTo(destination.plus(cacheName).getPath().toString());
        });

        context.getPipeline().run().waitUntilFinish();

        controller.deserializeDataFromAvro(destination.getPath());

        clientAPI.getIgnite().cache(cacheName).withKeepBinary()
                .query(new ScanQuery<Object, BinaryObject>())
                .forEach(entry -> {
                    BinaryObject passport = entry.getValue().field("passport");
                    BinaryObject affKey = ((BinaryObject)entry.getKey()).field("affKey");
                    Assert.assertEquals(affKey.field("id"), passportId);
                    Assert.assertEquals(passport.field("id"), passportIdChanged);
                });
    }
}
