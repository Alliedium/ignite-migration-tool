package org.alliedium.ignite.migration.serializer;

import org.alliedium.ignite.migration.serializer.utils.AvroFileNames;
import org.alliedium.ignite.migration.util.PathCombine;

import java.nio.file.Path;

public class CacheAvroFilesLocator {

    private final PathCombine rootCachePath;

    public CacheAvroFilesLocator(PathCombine rootCachePath) {
        this.rootCachePath = rootCachePath;
    }

    public Path cacheConfigurationSchemaPath() {
        return rootCachePath.plus(AvroFileNames.SCHEMA_FOR_CACHE_CONFIGURATION_FILENAME).getPath();
    }

    public Path cacheConfigurationPath() {
        return rootCachePath.plus(AvroFileNames.CACHE_CONFIGURATION_FILENAME).getPath();
    }

    public Path cacheDataSchemaPath() {
        return rootCachePath.plus(AvroFileNames.SCHEMA_FOR_CACHE_DATA_FILENAME).getPath();
    }

    public Path cacheDataPath() {
        return rootCachePath.plus(AvroFileNames.CACHE_DATA_FILENAME).getPath();
    }
}
