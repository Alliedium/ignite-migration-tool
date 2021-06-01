package org.alliedium.ignite.migration.dao;

import org.alliedium.ignite.migration.dao.converters.IIgniteDTOConverter;
import org.alliedium.ignite.migration.dao.converters.IgniteBinaryObjectConverter;
import org.alliedium.ignite.migration.dao.dataaccessor.IgniteCacheDAO;
import org.alliedium.ignite.migration.dto.ICacheData;
import org.alliedium.ignite.migration.dto.ICacheEntryValue;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
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
    private final Map<String, IgniteDataStreamer<Object, BinaryObject>> binaryStreamers = new HashMap<>();

    public IgniteCacheDataWriter(IIgniteDTOConverter<String, Object> cacheKeyConverter, Ignite ignite) {
        super(cacheKeyConverter, ignite);
    }

    @Override
    public void write(ICacheData data) {
        waitForCacheCreation(data.getCacheName());

        if (cacheNeedsToBeRestored(data.getCacheName())) {
            IgniteCacheDAO igniteCacheDAO = getCacheDAO(data.getCacheName());
            String igniteCacheValueType = igniteCacheDAO.getCacheValueType();
            Map.Entry<Object, BinaryObject> cacheData = getCacheDataForInsert(ignite, data, igniteCacheValueType);

            binaryStreamers.get(data.getCacheName())
                    .addData(cacheData.getKey(), cacheData.getValue());
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

    private void waitForCacheCreation(String cacheName) {
        if (binaryStreamers.containsKey(cacheName)) {
            return;
        }

        long startTime = System.currentTimeMillis();
        do {
            if (ignite.cacheNames().contains(cacheName)) {
                IgniteDataStreamer<Object, BinaryObject> binaryStreamer = ignite.dataStreamer(cacheName);
                // todo: this property can be handled by user
                // basically it means override value of keys which already exist or not.
                // by allowing overwrite option ignite data streamer becomes more like put and putAll
                // but faster.
                binaryStreamer.allowOverwrite(true);
                binaryStreamers.put(cacheName, binaryStreamer);
                return;
            }
            try {
                TimeUnit.MILLISECONDS.sleep(1);
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
        binaryStreamers.forEach((cacheName, binaryStreamer) -> binaryStreamer.close());
        binaryStreamers.clear();
    }
}
