package io.github.alliedium.ignite.migration.patchtools;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;

public class RenameCacheActionTest extends BaseTest {

    @Test
    public void testRenameCache(Method method) {
        String cacheName = method.getName();
        String newCacheName = cacheName + 1;
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
            action = new RenameCacheAction.Builder()
                    .action(action)
                    .newCacheName(newCacheName)
                    .newTableName("CITY1")
                    .build();
            new CacheWriter(action).writeTo(destination.plus(newCacheName).getPath().toString());
            context.markCacheResolved(cacheName);
        });

        context.getPipeline().run().waitUntilFinish();

        clientAPI.clearIgniteAndCheckIgniteIsEmpty();

        controller.deserializeDataFromAvro(destination.getPath());

        Set<String> cacheNames = new HashSet<>(clientAPI.getIgnite().cacheNames());
        Assert.assertFalse(cacheNames.contains(cacheName));
        Assert.assertTrue(cacheNames.contains(newCacheName));
    }
}
