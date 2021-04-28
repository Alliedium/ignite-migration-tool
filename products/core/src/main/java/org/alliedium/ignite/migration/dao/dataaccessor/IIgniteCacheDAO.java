package org.alliedium.ignite.migration.dao.dataaccessor;

import java.util.Collection;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;

/**
 * Provides an access to particular ignite cache data and configurations.
 */
public interface IIgniteCacheDAO {

    IgniteCache<Object, BinaryObject> getBinaryCache();

    BinaryObject getAnyValue();

    CacheConfiguration<?, ?> getCacheConfiguration();

    Collection<QueryEntity> getCacheQueryEntities();

    String getCacheValueType();

}
