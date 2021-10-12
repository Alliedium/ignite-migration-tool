package org.alliedium.ignite.migration.patches;

import org.alliedium.ignite.migration.demotools.CacheNames;
import org.alliedium.ignite.migration.patchtools.*;
import org.alliedium.ignite.migration.test.TestDirectories;
import org.alliedium.ignite.migration.util.PathCombine;
import org.apache.beam.sdk.values.Row;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;


/**
 * This patch works with two caches, it shows how to apply different actions to caches.
 * This patch adds a field to first cache and removes another field from second cache.
 * Any other cache are ignored by this patch.
 */
public class AlterCachesDemoPatch {

    private static final Random random = new Random();

    public static void main(String[] args) {
        // resolve source and destination folders
        TestDirectories testDirectories = new TestDirectories();
        Path sourcePath = testDirectories.getAvroTestSetPath();
        Path destinationPath = testDirectories.getAvroMainPath();
        if (args.length > 1) {
            sourcePath = Paths.get(args[0]);
            destinationPath = Paths.get(args[1]);
        }

        PathCombine rootDirectory = new PathCombine(sourcePath);
        PathCombine destinationDirectory = new PathCombine(destinationPath);
        PatchContext context = new PatchContext(rootDirectory, destinationDirectory);
        context.prepare();

        context.patchCachesWhichEndWith(CacheNames.FIRST, cachePath -> {
            TransformAction<TransformOutput> action = new SelectAction(context)
                    .fields("key", "name", "district", "population")
                    .from(cachePath);
            action = new CopyFieldAction(action)
                    .copyField("population", "age");
            action = new MapAction(action)
                    .map(row ->
                            Row.fromRow(row)
                                    .withFieldValue("age", random.nextInt())
                                    .build());
            String cacheName = cachePath.substring(cachePath.lastIndexOf("/"));
            new Writer(action).writeTo(destinationDirectory.plus(cacheName).getPath().toString());
        });

        context.patchCachesWhichEndWith(CacheNames.SECOND, cachePath -> {
            TransformAction<TransformOutput> action = new SelectAction(context)
                    .fields("key", "name", "district")
                    .from(cachePath);
            String cacheName = cachePath.substring(cachePath.lastIndexOf("/"));
            new Writer(action).writeTo(destinationDirectory.plus(cacheName).getPath().toString());
        });

        context.getPipeline().run().waitUntilFinish();
        context.copyAllNotTouchedFilesToOutput();
    }
}
