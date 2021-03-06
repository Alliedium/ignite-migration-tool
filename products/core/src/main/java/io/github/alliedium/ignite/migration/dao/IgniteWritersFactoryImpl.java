package io.github.alliedium.ignite.migration.dao;

import io.github.alliedium.ignite.migration.dao.converters.IIgniteDTOConverter;
import io.github.alliedium.ignite.migration.dao.converters.IgniteObjectStringConverter;

import java.util.*;

import io.github.alliedium.ignite.migration.IDataWriter;
import io.github.alliedium.ignite.migration.dto.ICacheMetaData;
import org.apache.ignite.Ignite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * IgniteWriter is responsible for Apache Ignite caches recreation and repopulating.
 * Apache Ignite connection (established to an Apache Ignite node where the caches need to be restored) needs to be provided.
 * Apache Ignite cache is being recreated and repopulated based on each DTO from the provided list.
 */
public class IgniteWritersFactoryImpl implements IgniteWritersFactory {

    private static final Logger logger = LoggerFactory.getLogger(IgniteWritersFactoryImpl.class);

    private final IIgniteDTOConverter<String, Object> cacheKeyConverter;
    private final Ignite ignite;

    public IgniteWritersFactoryImpl(Ignite ignite) {
        this.ignite = ignite;
        cacheKeyConverter = IgniteObjectStringConverter.GENERIC_CONVERTER;
    }

    public IDataWriter<ICacheMetaData> getIgniteCacheMetaDataWriter() {
        return new IgniteCacheMetaDataWriter(cacheKeyConverter, ignite);
    }

    public IgniteCacheDataWriter getIgniteCacheDataWriter() {
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
