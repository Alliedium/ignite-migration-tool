package io.github.alliedium.ignite.migration.patches;

import io.github.alliedium.ignite.migration.demotools.AlterDataCommon;
import io.github.alliedium.ignite.migration.demotools.CacheNames;
import io.github.alliedium.ignite.migration.patchtools.*;
import org.apache.beam.sdk.values.Row;

import java.io.IOException;
import java.util.*;


/**
 * This patch works with two caches, it shows how to apply different actions to caches.
 * This patch adds a field to first cache and removes another field from second cache.
 * Any other cache are ignored by this patch.
 */
public class AlterCachesDemoPatch extends AlterDataCommon {

    private static final Random random = new Random();

    public AlterCachesDemoPatch(String[] args) {
        super(args);
    }

    @Override
    protected void alterData() {
        context.patchCachesWhichEndWith(CacheNames.FIRST, cachePath -> {
            TransformAction<TransformOutput> action = new SelectAction.Builder()
                    .context(context)
                    .fields("key", "name", "district", "population")
                    .from(cachePath)
                    .build();
            action = new CopyFieldAction.Builder()
                    .action(action)
                    .copyField("population", "age")
                    .build();
            action = new MapAction.Builder()
                    .action(action)
                    .map(row ->
                            Row.fromRow(row)
                                    .withFieldValue("age", random.nextInt())
                                    .build())
                    .build();
            String cacheName = cachePath.substring(cachePath.lastIndexOf("/"));
            new CacheWriter(action).writeTo(destination.plus(cacheName).getPath().toString());
        });

        context.patchCachesWhichEndWith(CacheNames.SECOND, cachePath -> {
            TransformAction<TransformOutput> action = new SelectAction.Builder()
                    .context(context)
                    .fields("key", "name", "district")
                    .from(cachePath)
                    .build();
            String cacheName = cachePath.substring(cachePath.lastIndexOf("/"));
            new CacheWriter(action).writeTo(destination.plus(cacheName).getPath().toString());
        });

        context.getPipeline().run().waitUntilFinish();
        context.copyAllNotTouchedFilesToOutput();
    }

    public static void main(String[] args) throws IOException {
        new AlterCachesDemoPatch(args).execute();
    }
}
