package org.alliedium.ignite.migration.dao.extractors;

import org.alliedium.ignite.migration.IDispatcher;
import org.alliedium.ignite.migration.dao.dataaccessor.IIgniteCacheDAO;
import org.alliedium.ignite.migration.dto.ICacheData;
import org.apache.ignite.binary.BinaryObject;

public class IgniteDataExtractorFactory {

    public static IgniteDataExtractor create(String cacheName, IIgniteCacheDAO igniteCacheDAO,
                                      IDispatcher<ICacheData> cacheDataDispatcher) {
        if (igniteCacheDAO.getAnyKey() instanceof BinaryObject) {
            return new BinaryKeyIgniteDataExtractor(cacheName, igniteCacheDAO, cacheDataDispatcher);
        }

        return new PlainIgniteDataExtractor(cacheName, igniteCacheDAO, cacheDataDispatcher);
    }
}
