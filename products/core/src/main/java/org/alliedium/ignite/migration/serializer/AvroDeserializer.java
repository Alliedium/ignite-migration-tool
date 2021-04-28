package org.alliedium.ignite.migration.serializer;

import org.alliedium.ignite.migration.IDispatcher;
import org.alliedium.ignite.migration.Utils;
import org.alliedium.ignite.migration.dto.*;
import org.alliedium.ignite.migration.util.PathCombine;
import org.apache.ignite.cache.QueryEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

public class AvroDeserializer implements IAvroDeserializer {

    private static final Logger logger = LoggerFactory.getLogger(AvroDeserializer.class);

    private final PathCombine rootSerializedPath;

    public AvroDeserializer(Path rootSerializedPath) {
        this.rootSerializedPath = new PathCombine(rootSerializedPath);
    }

    @Override
    public void deserializeCaches(IDispatcher<ICacheMetaData> cacheMetaDataDispatcher, IDispatcher<ICacheData> cacheDataDispatcher) {
        try {
            List<Path> subdirectoryPathsList = Utils.getSubdirectoryPathsFromDirectory(rootSerializedPath.getPath());
            for (Path subdirectoryPath : subdirectoryPathsList) {
                deserializeAndDistributeCacheData(subdirectoryPath, cacheMetaDataDispatcher, cacheDataDispatcher);
            }
        } catch (IOException e) {
            logger.error("Failed to deserialize caches");
            throw new IllegalStateException(e);
        } finally {
            cacheMetaDataDispatcher.finish();
            cacheDataDispatcher.finish();
        }
    }

    @Override
    public void deserializeAtomicsLong(IDispatcher<Map.Entry<String, Long>> atomicsLongDispatcher) {
        try {
            AvroFileReader reader = new AvroFileReader(rootSerializedPath);
            reader.distributeAtomicsLongData(atomicsLongDispatcher);
        } catch (IOException e) {
            logger.error("Failed to deserialize atomic longs");
            throw new IllegalStateException(e);
        } finally {
            atomicsLongDispatcher.finish();
        }
    }

    private void deserializeAndDistributeCacheData(Path serializedCachePath, IDispatcher<ICacheMetaData> cacheMetaDataDispatcher,
                                                   IDispatcher<ICacheData> cacheDataDispatcher) throws IOException {
        IAvroFileReader avroFileReader = new AvroFileReader(new PathCombine(serializedCachePath));

        String cacheName = serializedCachePath.getFileName().toString();

        String deserializingCacheConfiguration = avroFileReader.getCacheConfiguration();
        ICacheConfigurationData cacheConfigurationDTO = new CacheConfigurationData(deserializingCacheConfiguration);

        String deserializingCacheEntryMeta = avroFileReader.getCacheEntryMeta();
        ICacheEntryMetaData cacheEntryMetaDTO = new CacheEntryMetaData(deserializingCacheEntryMeta);

        cacheMetaDataDispatcher.publish(new CacheMetaData(cacheName, cacheConfigurationDTO, cacheEntryMetaDTO));

        avroFileReader.distributeCacheData(cacheName, getFieldsTypes(cacheEntryMetaDTO), cacheDataDispatcher);
    }

    private Map<String, String> getFieldsTypes(ICacheEntryMetaData cacheEntryMeta) {
        Collection<QueryEntity> queryEntities = Utils.deserializeFromXML(cacheEntryMeta.toString());
        if (queryEntities.isEmpty()) {
            return new HashMap<>();
        }

        QueryEntity cacheQueryEntity = queryEntities.iterator().next();
        return cacheQueryEntity.getFields();
    }
}
