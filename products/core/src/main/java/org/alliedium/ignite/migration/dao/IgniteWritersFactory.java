package org.alliedium.ignite.migration.dao;

import java.util.Map;

import org.alliedium.ignite.migration.dto.ICacheData;
import org.alliedium.ignite.migration.dto.ICacheMetaData;
import org.alliedium.ignite.migration.IDataWriter;

/**
 * Recreates and repopulates ignite caches.
 * Ignite node used for caches restoring is regulated by the provided ignite connection.
 */
public interface IgniteWritersFactory {

    IDataWriter<ICacheMetaData> getIgniteCacheMetaDataWriter();

    IgniteCacheDataWriter getIgniteCacheDataWriter();

    IDataWriter<Map.Entry<String, Long>> getIgniteAtomicsLongDataWriter();
}
