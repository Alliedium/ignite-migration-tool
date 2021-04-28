package org.alliedium.ignite.migration.dao;

import org.alliedium.ignite.migration.IDispatcher;
import org.alliedium.ignite.migration.dao.dataaccessor.IIgniteDAO;
import java.util.Map;

import org.alliedium.ignite.migration.dto.ICacheData;
import org.alliedium.ignite.migration.dto.ICacheMetaData;

/**
 * Uses the provided ignite connection to access an ignite data, reads meta-data and data from
 * all the ignite caches and distributes data free from Ignite-specific class references.
 *
 * @see ICacheMetaData
 * @see ICacheData
 */
public interface IIgniteReader {

    void read(IIgniteDAO igniteDAO, IDispatcher<ICacheMetaData> cacheMetaDataDispatcher,
              IDispatcher<ICacheData> cacheDataDispatcher, IDispatcher<Map.Entry<String, Long>> atomicLongsDispatcher);

}
