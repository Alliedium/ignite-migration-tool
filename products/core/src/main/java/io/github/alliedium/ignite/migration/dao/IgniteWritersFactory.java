package io.github.alliedium.ignite.migration.dao;

import java.util.Map;

import io.github.alliedium.ignite.migration.IDataWriter;
import io.github.alliedium.ignite.migration.dto.ICacheMetaData;

/**
 * Recreates and repopulates ignite caches.
 * Ignite node used for caches restoring is regulated by the provided ignite connection.
 */
public interface IgniteWritersFactory {

    IDataWriter<ICacheMetaData> getIgniteCacheMetaDataWriter();

    IgniteCacheDataWriter getIgniteCacheDataWriter();

    IDataWriter<Map.Entry<String, Long>> getIgniteAtomicsLongDataWriter();
}
