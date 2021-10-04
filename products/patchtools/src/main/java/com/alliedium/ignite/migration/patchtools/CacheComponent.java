package com.alliedium.ignite.migration.patchtools;

import org.alliedium.ignite.migration.serializer.AvroFileReader;
import org.alliedium.ignite.migration.serializer.CacheAvroFilesLocator;
import org.alliedium.ignite.migration.util.PathCombine;

public class CacheComponent {
    private PathCombine cachePath;
    private CacheAvroFilesLocator filesLocator;
    private AvroFileReader avroFileReader;

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
