package org.alliedium.ignite.migration.dao;

import org.alliedium.ignite.migration.dao.converters.IIgniteDTOConverter;
import org.alliedium.ignite.migration.dao.converters.IgniteObjectStringConverter;

import java.util.*;

import org.alliedium.ignite.migration.IDataWriter;
import org.alliedium.ignite.migration.dto.ICacheData;
import org.alliedium.ignite.migration.dto.ICacheMetaData;
import org.apache.ignite.Ignite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * IgniteWriter is responsible for Ignite caches recreation and repopulating.
 * Ignite connection (established to an ignite node where the caches need to be restored) needs to be provided.
 * Ignite cache is being recreated and repopulated based on each DTO from the provided list.
 */
public class IgniteWritersFactoryImpl implements IgniteWritersFactory {

    private static final Logger logger = LoggerFactory.getLogger(IgniteWritersFactoryImpl.class);

    private final IIgniteDTOConverter<String, Object> cacheKeyConverter;
    private final Ignite ignite;

    public IgniteWritersFactoryImpl(Ignite ignite) {
        this.ignite = ignite;
        cacheKeyConverter = new IgniteObjectStringConverter();
    }

    public IDataWriter<ICacheMetaData> getIgniteCacheMetaDataWriter() {
        return new IgniteCacheMetaDataWriter(cacheKeyConverter, ignite);
    }

    public IDataWriter<ICacheData> getIgniteCacheDataWriter() {
        return new IgniteCacheDataWriter(cacheKeyConverter, ignite);
    }

    public IDataWriter<Map.Entry<String, Long>> getIgniteAtomicsLongDataWriter() {
        return entry -> {
            ignite.atomicLong(entry.getKey(), entry.getValue(), true);
            logger.info(String.format("Ignite atomic long was recreated successfully with name: %s, and value: %d",
                    entry.getKey(), entry.getValue()));
        };
    }
}
