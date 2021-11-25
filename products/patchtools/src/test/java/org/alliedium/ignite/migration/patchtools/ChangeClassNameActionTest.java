package org.alliedium.ignite.migration.patchtools;

import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

public class ChangeClassNameActionTest extends BaseTest {

    @Test
    public void testChangeClassNameAction(Method method) {
        String cacheName = method.getName();
        createCityCache(cacheName);
        controller.serializeDataToAvro(source.getPath());
        clientAPI.clearIgniteAndCheckIgniteIsEmpty();

        PatchContext context = new PatchContext(source, destination);
        context.prepare();

        context.patchCachesWhichEndWith(cacheName, cachePath -> {
            TransformAction<TransformOutput> action = new SelectAction.Builder()
                    .context(context)
                    .fields("key", "name", "district", "population")
                    .from(cachePath)
                    .build();

            action = new ChangeClassNameAction.Builder()
                    .action(action)
                    .changeClassName("org.alliedium.ignite.migration.test.model.City",
                            "org.alliedium.ignite.migration.changed.test.model.City")
                    .build();

            new CacheWriter(action).writeTo(destination.plus(cacheName).getPath().toString());
        });

        context.getPipeline().run().waitUntilFinish();

        controller.deserializeDataFromAvro(destination.getPath());

        QueryEntity queryEntity = (QueryEntity) clientAPI.getIgnite().cache(cacheName)
                .getConfiguration(CacheConfiguration.class).getQueryEntities().iterator().next();

        Assert.assertEquals(queryEntity.getValueType(), "org.alliedium.ignite.migration.changed.test.model.City");
    }
}
