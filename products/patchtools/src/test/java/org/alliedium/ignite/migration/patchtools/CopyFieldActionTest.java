package org.alliedium.ignite.migration.patchtools;

import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.ScanQuery;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

public class CopyFieldActionTest extends BaseTest {

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
}
