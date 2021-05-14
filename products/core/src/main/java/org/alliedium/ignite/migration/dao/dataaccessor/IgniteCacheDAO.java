package org.alliedium.ignite.migration.dao.dataaccessor;

import java.util.Collection;
import java.util.Iterator;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;

import javax.cache.Cache;

/**
 * Provides an access to particular Apache Ignite cache data and configurations.
 * Unit needs to be provided with an Apache Ignite connection and required cache name on on initialization.
 */
public class IgniteCacheDAO implements IIgniteCacheDAO {

    private final IgniteCache<Object, BinaryObject> binaryCache;

    public IgniteCacheDAO(Ignite ignite, String cacheName) {
        this.binaryCache = ignite.cache(cacheName).withKeepBinary();
    }

    public IgniteCache<Object, BinaryObject> getBinaryCache() {
        return this.binaryCache;
    }

    @Override
    public BinaryObject getAnyValue() {
        Iterator<Cache.Entry<Object, BinaryObject>> iterator = binaryCache.iterator();
        if (iterator.hasNext()) {
            return iterator.next().getValue();
        }

        throw new IllegalStateException("provided ignite cache is empty");
    }

    @SuppressWarnings("unchecked")
    public CacheConfiguration<Object, BinaryObject> getCacheConfiguration() {
        return this.binaryCache.getConfiguration(CacheConfiguration.class);
    }

    public Collection<QueryEntity> getCacheQueryEntities() {
        return getCacheConfiguration().getQueryEntities();
    }

    public String getCacheValueType() {
        Iterator<QueryEntity> queryEntityIterator = getCacheQueryEntities().iterator();
        if (queryEntityIterator.hasNext()) {
            return queryEntityIterator.next().findValueType();
        }

        throw new IllegalStateException("ignite migration tool does not work without ignite cache query entities");
    }

}
