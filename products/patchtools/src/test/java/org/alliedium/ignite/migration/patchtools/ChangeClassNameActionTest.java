package org.alliedium.ignite.migration.patchtools;

import org.alliedium.ignite.migration.test.model.City;
import org.alliedium.ignite.migration.util.BinaryObjectUtil;
import org.apache.ignite.binary.BinaryObject;
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
        applyChangeClassNameActionPatchAndCheckResult(cacheName, true);
    }

    @Test
    public void testChangeClassNameActionWithoutQueryEntities(Method method) {
        String cacheName = method.getName();
        CacheConfiguration<Integer, City> configuration = new CacheConfiguration<>();
        configuration.setName(cacheName);
        clientAPI.createCacheAndFillWithData(configuration,
                () -> new City("newCity", "districtA", 1000), 10);
        applyChangeClassNameActionPatchAndCheckResult(cacheName, false);
    }

    private void applyChangeClassNameActionPatchAndCheckResult(String cacheName, boolean checkQueryEntities) {
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

        if (checkQueryEntities) {
            QueryEntity queryEntity = (QueryEntity) clientAPI.getIgnite().cache(cacheName)
                    .getConfiguration(CacheConfiguration.class).getQueryEntities().iterator().next();

            Assert.assertEquals(queryEntity.getValueType(), "org.alliedium.ignite.migration.changed.test.model.City");
        }

        BinaryObject val = (BinaryObject) clientAPI.getIgnite().cache(cacheName)
                .withKeepBinary().iterator().next().getValue();
        String typeName = BinaryObjectUtil.getBinaryTypeName(val);

        Assert.assertEquals(typeName, "org.alliedium.ignite.migration.changed.test.model.City");
    }
}
