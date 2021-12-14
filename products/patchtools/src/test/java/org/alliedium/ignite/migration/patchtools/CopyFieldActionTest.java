package org.alliedium.ignite.migration.patchtools;

import org.alliedium.ignite.migration.test.TestUtils;
import org.alliedium.ignite.migration.test.model.IdContainer;
import org.alliedium.ignite.migration.test.model.Passport;
import org.alliedium.ignite.migration.test.model.Person;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

public class CopyFieldActionTest extends IsolatedIgniteNodePerTest {

    @Test
    public void testCopyFieldAction(Method method) {
        String cacheName = method.getName();
        createCityCache(cacheName);
        controller.serializeDataToAvro(source.getPath());

        PatchContext context = new PatchContext(source, destination);
        context.prepare();

        context.patchCachesWhichEndWith(cacheName, cachePath -> {
            TransformAction<TransformOutput> action = new SelectAction.Builder()
                    .context(context)
                    .fields("key", "name", "district", "population")
                    .from(cachePath)
                    .build();

            action = new CopyFieldAction.Builder()
                    .action(action)
                    .copyField("population", "age")
                    .build();

            new CacheWriter(action).writeTo(destination.plus(cacheName).getPath().toString());
        });

        context.getPipeline().run().waitUntilFinish();

        controller.deserializeDataFromAvro(destination.getPath());
        clientAPI.getIgnite().cache(cacheName).withKeepBinary()
                .query(new ScanQuery<Object, BinaryObject>())
                .forEach(entry -> {
                    Assert.assertTrue(entry.getValue().hasField("population"));
                    Assert.assertTrue(entry.getValue().hasField("age"));
                    Assert.assertEquals((int) entry.getValue().field("population"),
                            (int) entry.getValue().field("age"));
                });
    }

    @Test
    public void testCopyNestedField(Method method) {
        // todo: try to run this code two times and you will get error, because ignite will remember field `secondPassport`
        // which was created at first run. This should be fixed, because the same issue can appear in other tests
        String cacheName = TestUtils.getMethodName(getClass(), method);
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
                    .fields("key", "name", "passport", "age")
                    .from(cachePath)
                    .build();
            action = new CopyFieldAction.Builder()
                    .action(action)
                    .copyField("passport", "secondPassport")
                    .build();

            new CacheWriter(action).writeTo(destination.plus(cacheName).getPath().toString());
        });

        context.getPipeline().run().waitUntilFinish();

        controller.deserializeDataFromAvro(destination.getPath());

        clientAPI.getIgnite().cache(cacheName).withKeepBinary()
                .query(new ScanQuery<Object, BinaryObject>())
                .forEach(entry -> {
                    BinaryObject passport = entry.getValue().field("passport");
                    BinaryObject secondPassport = entry.getValue().field("secondPassport");
                    String passportId = passport.field("id");
                    String secondPassportId = secondPassport.field("id");

                    Assert.assertEquals(passportId, secondPassportId);
                });
    }
}
