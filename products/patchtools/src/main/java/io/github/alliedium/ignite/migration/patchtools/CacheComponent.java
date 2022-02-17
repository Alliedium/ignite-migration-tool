package io.github.alliedium.ignite.migration.patchtools;

import io.github.alliedium.ignite.migration.util.PathCombine;
import io.github.alliedium.ignite.migration.serializer.AvroFileReader;
import io.github.alliedium.ignite.migration.serializer.CacheAvroFilesLocator;

public class CacheComponent {
    private final PathCombine cachePath;
    private final CacheAvroFilesLocator filesLocator;
    private final AvroFileReader avroFileReader;

    public CacheComponent(PathCombine cachePath) {
        this.cachePath = cachePath;
        this.filesLocator = new CacheAvroFilesLocator(cachePath);
        this.avroFileReader = new AvroFileReader(cachePath);
    }

    public PathCombine getCachePath() {
        return cachePath;
    }

    public CacheAvroFilesLocator getFilesLocator() {
        return filesLocator;
    }

    public AvroFileReader getAvroFileReader() {
        return avroFileReader;
    }
}
