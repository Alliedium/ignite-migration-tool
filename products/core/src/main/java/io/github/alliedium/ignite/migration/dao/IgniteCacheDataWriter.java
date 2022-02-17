package io.github.alliedium.ignite.migration.dao;

import io.github.alliedium.ignite.migration.IDataWriter;
import io.github.alliedium.ignite.migration.dao.converters.BinaryObjectConverter;
import io.github.alliedium.ignite.migration.dto.CacheDataTypes;
import io.github.alliedium.ignite.migration.serializer.utils.FieldNames;
import io.github.alliedium.ignite.migration.dao.converters.IIgniteDTOConverter;
import io.github.alliedium.ignite.migration.dao.dataaccessor.IIgniteCacheDAO;
import io.github.alliedium.ignite.migration.dao.dataaccessor.IgniteCacheDAO;
import io.github.alliedium.ignite.migration.dto.ICacheData;
import io.github.alliedium.ignite.migration.dto.ICacheEntryValue;
import io.github.alliedium.ignite.migration.dto.ICacheMetaData;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.binary.BinaryObject;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class IgniteCacheDataWriter extends IgniteDataWriter implements IDataWriter<ICacheData> {

    private static final int CACHE_CREATION_TIMEOUT_SECONDS = 30;
    private final Map<String, IgniteCacheDAO> cacheDAOMap = new HashMap<>();
    private final Map<String, IgniteDataStreamer<Object, BinaryObject>> binaryStreamers = new HashMap<>();
    private final MetaDataConsumer metaDataConsumer;

    public IgniteCacheDataWriter(IIgniteDTOConverter<String, Object> cacheKeyConverter, Ignite ignite) {
        super(cacheKeyConverter, ignite);
        this.metaDataConsumer = new MetaDataConsumer();
    }

    @Override
    public void write(ICacheData data) {
        waitForCacheCreation(data.getCacheName());

        if (cacheNeedsToBeRestored(data.getCacheName())) {
            IgniteCacheDAO igniteCacheDAO = getCacheDAO(data.getCacheName());
            Map.Entry<Object, BinaryObject> cacheData = getCacheDataForInsert(ignite, data, igniteCacheDAO);

            binaryStreamers.get(data.getCacheName())
                    .addData(cacheData.getKey(), cacheData.getValue());
        }
        else {
            logger.info("Skipping cache refilling for already populated one: " + data.getCacheName());
        }
    }

    public MetaDataConsumer getMetaDataConsumer() {
        return this.metaDataConsumer;
    }

    private IgniteCacheDAO getCacheDAO(String cacheName) {
        if (cacheDAOMap.get(cacheName) == null) {
            IgniteCacheDAO cacheDAO = new IgniteCacheDAO(ignite, cacheName);
            cacheDAOMap.put(cacheName, cacheDAO);
        }

        return cacheDAOMap.get(cacheName);
    }

    private Map.Entry<Object, BinaryObject> getCacheDataForInsert(Ignite ignite, ICacheData cacheData, IIgniteCacheDAO cacheDAO) {
        CacheDataTypes cacheDataTypes = metaDataConsumer.metaDataMap.get(cacheData.getCacheName()).getTypes();
        String cacheValueType = cacheDataTypes.getValType();
        String cacheKeyType = cacheDataTypes.getKeyType();
        IIgniteDTOConverter<ICacheEntryValue, BinaryObject> valConverter = new BinaryObjectConverter(ignite, cacheValueType);
        IIgniteDTOConverter<ICacheEntryValue, BinaryObject> keyConverter = new BinaryObjectConverter(ignite, cacheKeyType);
        BinaryObject value = valConverter.convertFromDTO(cacheData.getCacheEntryValue());
        Object key = parseKey(cacheData.getCacheEntryKey(), keyConverter);

        return new AbstractMap.SimpleEntry<>(key, value);
    }

    private Object parseKey(ICacheEntryValue key, IIgniteDTOConverter<ICacheEntryValue, BinaryObject> binaryObjectConverter) {
        List<String> keyFieldNames = key.getFieldNames();

        if (keyFieldNames.size() == 1 && keyFieldNames.get(0).equals(FieldNames.KEY_FIELD_NAME)) {
            Optional<Object> optional = key.getField(FieldNames.KEY_FIELD_NAME).getFieldValue();
            // an object can have null value, that's why we return null here, anyway this is strange for key
            // todo: figure out more soft way
            return optional.orElse(null);
        }

        return binaryObjectConverter.convertFromDTO(key);
    }

    private void waitForCacheCreation(String cacheName) {
        if (binaryStreamers.containsKey(cacheName)) {
            return;
        }

        long startTime = System.currentTimeMillis();
        do {
            if (ignite.cacheNames().contains(cacheName) && metaDataConsumer.metaDataMap.containsKey(cacheName)) {
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

    public static class MetaDataConsumer implements IDataWriter<ICacheMetaData> {
        private final Map<String, ICacheMetaData> metaDataMap = new ConcurrentHashMap<>();

        @Override
        public void write(ICacheMetaData data) {
            metaDataMap.put(data.getCacheName(), data);
        }
    }

    @Override
    public void close() {
        cacheDAOMap.clear();
        binaryStreamers.forEach((cacheName, binaryStreamer) -> binaryStreamer.close());
        binaryStreamers.clear();
        metaDataConsumer.metaDataMap.clear();
    }
}
