package org.alliedium.ignite.migration.dao;

import org.alliedium.ignite.migration.dao.converters.IIgniteDTOConverter;
import org.alliedium.ignite.migration.dao.converters.IgniteBinaryObjectConverter;
import org.alliedium.ignite.migration.dao.dataaccessor.IgniteCacheDAO;
import org.alliedium.ignite.migration.dto.ICacheData;
import org.alliedium.ignite.migration.dto.ICacheEntryValue;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.transactions.Transaction;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

public class IgniteCacheDataWriter extends IgniteDataWriter<ICacheData> {

    private static final int CACHE_CREATION_TIMEOUT_SECONDS = 30;
    private final Map<String, IgniteCacheDAO> cacheDAOMap = new HashMap<>();

    public IgniteCacheDataWriter(IIgniteDTOConverter<String, Object> cacheKeyConverter, Ignite ignite) {
        super(cacheKeyConverter, ignite);
    }

    @Override
    public void write(ICacheData data) {
        waitForCacheCreation(data.getCacheName());

        if (cacheNeedsToBeRestored(data.getCacheName()) || isCacheEmpty(data.getCacheName())) {
            IgniteCacheDAO igniteCacheDAO = getCacheDAO(data.getCacheName());
            IgniteCache<Object, BinaryObject> igniteCache = igniteCacheDAO.getBinaryCache();
            String igniteCacheValueType = igniteCacheDAO.getCacheValueType();
            Map.Entry<Object, BinaryObject> cacheData = getCacheDataForInsert(ignite, data, igniteCacheValueType);
            try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                igniteCache.put(cacheData.getKey(), cacheData.getValue());
                tx.commit();
            }
            catch (Exception e) {
                logger.warn("FAILED to repopulate cache item " + igniteCache.getName() + " due to an error: " + e.getMessage());
            }
        }
        else {
            logger.info("Skipping cache refilling for already populated one: " + data.getCacheName());
        }
    }

    private IgniteCacheDAO getCacheDAO(String cacheName) {
        if (cacheDAOMap.get(cacheName) == null) {
            IgniteCacheDAO cacheDAO = new IgniteCacheDAO(ignite, cacheName);
            cacheDAOMap.put(cacheName, cacheDAO);
        }

        return cacheDAOMap.get(cacheName);
    }

    private Map.Entry<Object, BinaryObject> getCacheDataForInsert(Ignite ignite, ICacheData cacheData, String cacheValueType) {
        IIgniteDTOConverter<ICacheEntryValue, BinaryObject> binaryObjectFromConverter = new IgniteBinaryObjectConverter(ignite, cacheValueType);
        BinaryObject binaryObjectFromEntryValue = binaryObjectFromConverter.convertFromDto(cacheData.getCacheEntryValue());
        String cacheEntryKeyString = cacheData.getCacheEntryKey().toString();

        return new AbstractMap.SimpleEntry<>(cacheKeyConverter.convertFromDto(cacheEntryKeyString), binaryObjectFromEntryValue);
    }

    private boolean isCacheEmpty(String cacheName) {
        return ignite.cache(cacheName) == null || ignite.cache(cacheName).size() == 0;
    }

    private void waitForCacheCreation(String cacheName) {
        long startTime = System.currentTimeMillis();
        do {
            if (ignite.cacheNames().contains(cacheName)) {
                return;
            }
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        } while(System.currentTimeMillis() - startTime <= TimeUnit.SECONDS.toMillis(CACHE_CREATION_TIMEOUT_SECONDS));

        throw new IllegalStateException(
                String.format("Waiting time (%d seconds) for cache creation expired, cache name: %s",
                        CACHE_CREATION_TIMEOUT_SECONDS, cacheName));
    }

    @Override
    public void close() {
        cacheDAOMap.clear();
    }
}
