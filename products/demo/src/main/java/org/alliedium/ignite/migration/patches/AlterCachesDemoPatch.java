package org.alliedium.ignite.migration.patches;

import org.alliedium.ignite.migration.patchtools.*;
import org.alliedium.ignite.migration.dto.ICacheData;
import org.alliedium.ignite.migration.dto.ICacheMetaData;
import org.alliedium.ignite.migration.test.TestDirectories;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;


/**
 * This patch works with two caches, it shows how to apply different actions to caches.
 * This patch adds a field to first cache and removes another field from second cache.
 * Any other cache are ignored by this patch.
 */
public class AlterCachesDemoPatch implements IPatch {

    private static final String fieldNameToAdd = "age";
    private static final String fieldNameToRemove = "population";
    private static final Random random = new Random();

    @Override
    public ICacheMetaData transformMetaData(ICacheMetaData cacheMetaData) {
        if (cacheMetaData.getCacheName().equals(CacheNames.FIRST)) {
            return new MetaDataTransformer(cacheMetaData)
                    .addFieldType(fieldNameToAdd, Integer.class).build();
        }
        if (cacheMetaData.getCacheName().equals(CacheNames.SECOND)) {
            return new MetaDataTransformer(cacheMetaData)
                    .removeFieldType(fieldNameToRemove).build();
        }

        // do nothing for other caches
        return cacheMetaData;
    }

    @Override
    public ICacheData transformData(ICacheData cacheData) {
        if (cacheData.getCacheName().equals(CacheNames.FIRST)) {
            return new CacheDataTransformer(cacheData)
                    .addField(fieldNameToAdd, random.nextInt()).build();
        }
        if (cacheData.getCacheName().equals(CacheNames.SECOND)) {
            return new CacheDataTransformer(cacheData)
                    .removeField(fieldNameToRemove).build();
        }

        return new CacheDataTransformer(cacheData).convertFieldsToAvro().build();
    }

    public static void main(String[] args) throws Exception {
        // resolve source and destination folders
        TestDirectories testDirectories = new TestDirectories();
        Path sourcePath = testDirectories.getAvroTestSetPath();
        Path destinationPath = testDirectories.getAvroMainPath();
        if (args.length > 1) {
            sourcePath = Paths.get(args[0]);
            destinationPath = Paths.get(args[1]);
        }

        // create an instance of patch
        IPatch patch = new AlterCachesDemoPatch();

        // provide source. destination and patch to patch processor
        PatchProcessor processor = new PatchProcessor(sourcePath, destinationPath, patch);

        // start processor
        processor.process();
    }
}
