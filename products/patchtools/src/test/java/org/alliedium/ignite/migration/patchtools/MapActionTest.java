package org.alliedium.ignite.migration.patchtools;

import org.apache.beam.sdk.values.Row;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.ScanQuery;
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
            TransformAction<TransformOutput> action = new SelectAction(context)
                    .fields("key", "name", "district", "population")
                    .from(cachePath);

            action = new MapAction(action)
                    .map(row -> Row.fromRow(row)
                            .withFieldValue("population", populationValue)
                            .build());

            new Writer(action).writeTo(destination.plus(cacheName).getPath().toString());
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
}
