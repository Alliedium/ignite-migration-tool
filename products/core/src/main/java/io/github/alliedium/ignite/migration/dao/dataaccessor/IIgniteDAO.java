package io.github.alliedium.ignite.migration.dao.dataaccessor;

import java.util.List;

/**
 * Provides an access to Apache Ignite caches (represented as {@link IIgniteCacheDAO}) and cache's meta-data.
 * Caches' meta-data can be requested by cache name, list of which can also be returned by current unit.
 * IIgniteDAO is used to be an 'entry point' for getting data from ignite.
 */
public interface IIgniteDAO {

    List<String> getCacheNames();

    boolean cacheIsEmpty(String cacheName);

    IIgniteCacheDAO getIgniteCacheDAO(String cacheName);

}
