package org.alliedium.ignite.migration.dao;

import org.alliedium.ignite.migration.dao.converters.IIgniteDTOConverter;
import org.alliedium.ignite.migration.IDataWriter;
import org.apache.ignite.Ignite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public abstract class IgniteDataWriter {

    //TODO: properties like below need to be outlined in separate file (yaml)
    // this is a way cache could be skipped
    private static final List<String> CACHE_NAMES_TO_SKIP_IF_EXIST = Arrays.asList(
            "any_cache_name_you_want_to_skip_1", "any_cache_name_you_want_to_skip_2");
    protected static final Logger logger = LoggerFactory.getLogger(IgniteCacheMetaDataWriter.class);

    protected final IIgniteDTOConverter<String, Object> cacheKeyConverter;
    protected final Ignite ignite;

    public IgniteDataWriter(IIgniteDTOConverter<String, Object> cacheKeyConverter, Ignite ignite) {
        this.cacheKeyConverter = cacheKeyConverter;
        this.ignite = ignite;
    }

    protected boolean cacheNeedsToBeRestored(String restoringCacheName) {
        return !CACHE_NAMES_TO_SKIP_IF_EXIST.contains(restoringCacheName);
    }
}
