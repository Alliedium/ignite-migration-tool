package org.alliedium.ignite.migration.dao.dataaccessor;

import java.util.*;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;

/**
 * Unit provides as access to ignite caches (represented as {@link IIgniteCacheDAO}) and
 * cache's meta-data (name, size, existence, atomic long values {@link Ignite#atomicLong(String, long, boolean)}).
 * Caches' meta-data can be requested by cache name, list of which can also be returned by current unit.
 * Unit needs to be provided with ignite connection on initialization.
 */
public class IgniteDAO implements IIgniteDAO {

    private final Ignite ignite;

    public IgniteDAO(Ignite ignite) {
        this.ignite = ignite;
    }

    public List<String> getCacheNames() {
        return new ArrayList<>(ignite.cacheNames());
    }

    public boolean cacheIsEmpty(String cacheName) {
        IgniteCache cache = ignite.cache(cacheName);
        return cache == null || cache.size() == 0;
    }

    public IIgniteCacheDAO getIgniteCacheDAO(String cacheName) {
        return new IgniteCacheDAO(ignite, cacheName);
    }
}
