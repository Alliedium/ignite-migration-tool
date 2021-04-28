package org.alliedium.ignite.migration.patches;

import org.alliedium.ignite.migration.patchtools.CacheDataTransformer;
import org.alliedium.ignite.migration.patchtools.IPatch;
import org.alliedium.ignite.migration.patchtools.MetaDataTransformer;
import org.alliedium.ignite.migration.patchtools.PatchProcessor;
import org.alliedium.ignite.migration.dto.ICacheData;
import org.alliedium.ignite.migration.dto.ICacheMetaData;

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

    public static void main(String[] args) throws Exception {
        // create an instance of patch
        IPatch patch = new AlterCacheAddFieldPatch();

        // provide patch for patch processor
        PatchProcessor processor = new PatchProcessor(patch);
        if (args.length > 1) {
            Path sourcePath = Paths.get(args[0]);
            Path destinationPath = Paths.get(args[1]);
            processor = new PatchProcessor(sourcePath, destinationPath, patch);
        }

        // start processor
        processor.process();
    }
}
