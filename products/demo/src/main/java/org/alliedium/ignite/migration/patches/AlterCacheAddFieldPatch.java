package org.alliedium.ignite.migration.patches;

import org.alliedium.ignite.migration.patchtools.CacheDataTransformer;
import org.alliedium.ignite.migration.patchtools.IPatch;
import org.alliedium.ignite.migration.patchtools.MetaDataTransformer;
import org.alliedium.ignite.migration.patchtools.PatchProcessor;
import org.alliedium.ignite.migration.dto.ICacheData;
import org.alliedium.ignite.migration.dto.ICacheMetaData;
import org.alliedium.ignite.migration.test.TestDirectories;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class AlterCacheAddFieldPatch implements IPatch {

    private static final String fieldName = "age";
    private static final Random random = new Random();

    @Override
    public ICacheMetaData transformMetaData(ICacheMetaData cacheMetaData) {
        return new MetaDataTransformer(cacheMetaData)
                .addFieldType(fieldName, Integer.class).build();
    }

    @Override
    public ICacheData transformData(ICacheData cacheData) {
        return new CacheDataTransformer(cacheData)
                .addField(fieldName, random.nextInt()).build();
    }

    public static void main(String[] args) {
        // resolve source and destination folders
        TestDirectories testDirectories = new TestDirectories();
        Path sourcePath = testDirectories.getAvroTestSetPath();
        Path destinationPath = testDirectories.getAvroMainPath();
        if (args.length > 1) {
            sourcePath = Paths.get(args[0]);
            destinationPath = Paths.get(args[1]);
        }

        // create an instance of patch
        IPatch patch = new AlterCacheAddFieldPatch();

        // provide source. destination and patch to patch processor
        PatchProcessor processor = new PatchProcessor(sourcePath, destinationPath, patch);

        // start processor
        processor.process();
    }
}
